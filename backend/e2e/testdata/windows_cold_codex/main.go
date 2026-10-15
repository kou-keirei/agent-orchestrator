// A disposable local protocol peer, never a model or provider qualification.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func record(kind, id string) {
	cwd, _ := os.Getwd()
	root := os.Getenv("AO_COLD_WORKSPACE_ROOT")
	rel, err := filepath.Rel(root, cwd)
	if root == "" || err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return
	}
	f, err := os.OpenFile(os.Getenv("AO_COLD_CALLS"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if _, err := fmt.Fprintf(f, "%s %s\n", kind, id); err != nil {
		panic(err)
	}
}

func main() {
	if len(os.Args) < 2 || os.Args[1] != "app-server" {
		fmt.Println("codex-cli 0.146.0")
		return
	}
	record("process", "app-server")
	scan := bufio.NewScanner(os.Stdin)
	scan.Buffer(make([]byte, 4096), 4<<20)
	enc := json.NewEncoder(os.Stdout)
	threadID := ""
	for scan.Scan() {
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params struct {
				ThreadID string `json:"threadId"`
				ClientID string `json:"clientUserMessageId"`
			} `json:"params"`
		}
		if err := json.Unmarshal(scan.Bytes(), &request); err != nil {
			panic(err)
		}
		if len(request.ID) == 0 {
			continue
		}
		var result any = map[string]any{}
		switch request.Method {
		case "thread/resume":
			threadID = request.Params.ThreadID
			record("resume", threadID)
			result = map[string]any{"thread": map[string]any{"id": threadID}, "model": "gpt-5.4", "approvalPolicy": "never", "sandbox": map[string]any{"type": "dangerFullAccess"}}
		case "thread/read":
			result = map[string]any{"thread": map[string]any{"id": request.Params.ThreadID, "turns": []any{}}}
		case "thread/start":
			panic("cold wake must never invent a native conversation")
		case "turn/start":
			record("send", request.Params.ThreadID+" "+request.Params.ClientID)
			result = map[string]any{"turn": map[string]any{"id": request.Params.ClientID}}
		case "model/list", "skills/list", "mcpServerStatus/list":
			result = map[string]any{"data": []any{}}
		}
		if err := enc.Encode(map[string]any{"id": request.ID, "result": result}); err != nil {
			return
		}
		if request.Method == "turn/start" {
			// Complete immediately; the controller handles notifications racing
			// its turn/start response and drains any accepted queue in order.
			_ = enc.Encode(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": threadID, "turn": map[string]any{"id": request.Params.ClientID, "status": "completed", "items": []any{}}}})
		}
	}
}
