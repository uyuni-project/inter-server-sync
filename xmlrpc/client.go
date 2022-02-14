package xmlrpc

import (
	"github.com/uyuni-project/hub-xmlrpc-api/config"
	"github.com/uyuni-project/hub-xmlrpc-api/uyuni"
	"github.com/uyuni-project/hub-xmlrpc-api/uyuni/client"
)

type xmlRpcClient struct {
	username string
	password string
}

func NewClient(username string, password string) *xmlRpcClient {
	return &xmlRpcClient{
		username,
		password,
	}
}

func (xmlRpc *xmlRpcClient) SyncConfigFiles(labels []string) (interface{}, error) {
	method := "configchannel.syncSaltFilesOnDisk"

	cfg := config.NewConfig()
	cl := client.NewClient(cfg.ConnectTimeout, cfg.RequestTimeout)
	exec := uyuni.NewUyuniCallExecutor(cl)

	auth := uyuni.NewUyuniAuthenticator(exec)
	token, err := auth.Login(cfg.HubAPIURL, xmlRpc.username, xmlRpc.password)
	resp, err := exec.ExecuteCall(cfg.HubAPIURL, method, []interface{}{token, labels})

	return resp, err
}
