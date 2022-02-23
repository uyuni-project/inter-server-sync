package xmlrpc

import (
	"context"
	"github.com/uyuni-project/xmlrpc-public-methods"
	"net"
	"net/http"
	"time"
)

const (
	ConnectTimeout = 10
	RequestTimeout = 10
	Endpoint       = "http://localhost/rpc/api"
	AuthMethod     = "auth.login"
	SyncMethod     = "configchannel.syncSaltFilesOnDisk"
)

type Client interface {
	SyncConfigFiles(labels []string) (interface{}, error)
}

type client struct {
	connectTimeout int
	requestTimeout int
	endpoint       string
	username       string
	password       string
}

func NewClient(username string, password string) *client {
	return &client{
		ConnectTimeout,
		RequestTimeout,
		Endpoint,
		username,
		password,
	}
}

func (c *client) executeCall(endpoint string, call string, args []interface{}) (response interface{}, err error) {
	client, err := getClientWithTimeout(endpoint, c.connectTimeout, c.requestTimeout)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	err = client.Call(call, args, &response)
	return response, err
}

func getClientWithTimeout(url string, connectTimeout, requestTimeout int) (*xmlrpc.Client, error) {
	transport := http.Transport{
		DialContext: timeoutDialer(time.Duration(connectTimeout)*time.Second, time.Duration(requestTimeout)*time.Second),
	}
	return xmlrpc.NewClient(url, &transport)
}

func timeoutDialer(connectTimeout, requestTimeout time.Duration) func(ctx context.Context, net, addr string) (c net.Conn, err error) {
	return func(ctx context.Context, netw, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(netw, addr, connectTimeout)
		if err != nil {
			return nil, err
		}
		conn.SetDeadline(time.Now().Add(requestTimeout))
		return conn, nil
	}
}

func (c *client) SyncConfigFiles(labels []string) (interface{}, error) {

	credentials := []interface{}{c.username, c.password}
	token, err := c.executeCall(c.endpoint, AuthMethod, credentials)
	if err != nil {
		return nil, err
	}
	syncPayload := []interface{}{token, labels}
	return c.executeCall(c.endpoint, SyncMethod, syncPayload)
}
