package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/kubo/client/rpc"
	"github.com/ipfs/kubo/config"
	"github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/coreapi"
	iface "github.com/ipfs/kubo/core/coreiface"
	"github.com/ipfs/kubo/core/node/libp2p"
	"github.com/ipfs/kubo/plugin/loader"
	"github.com/ipfs/kubo/repo"
	"github.com/ipfs/kubo/repo/fsrepo"
	"io"
	"net/http"
	"os"
)

var MAX_IPFS_FILESIZE int64 = 1024 * 1024

type IpfsDownloader struct {
	repository *repo.Repo
	node       *core.IpfsNode
	api        iface.CoreAPI
}

func NewEmbeddedIpfsDownloader() (downloader *IpfsDownloader, err error) {
	ctx := context.Background()
	repoPath, err := os.MkdirTemp("", "ipfs-repo")
	plugins, err := loader.NewPluginLoader(repoPath)
	if err != nil {
		return nil, err
	}
	if err := plugins.Initialize(); err != nil {
		return nil, err
	}
	if err := plugins.Inject(); err != nil {
		return nil, err

	}
	cfg, err := config.Init(io.Discard, 2048)
	if err != nil {
		return nil, err
	}
	cfg.Addresses.Swarm = []string{}
	cfg.Discovery.MDNS.Enabled = false
	if err := fsrepo.Init(repoPath, cfg); err != nil {
		return nil, err
	}
	repository, err := fsrepo.Open(repoPath)
	if err != nil {
		return nil, err
	}

	node, err := core.NewNode(ctx, &core.BuildCfg{
		Online:  true,
		Repo:    repository,
		Routing: libp2p.DHTClientOption,
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			node.Close()
		}
	}()
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		return nil, err
	}
	downloader = &IpfsDownloader{
		repository: &repository,
		node:       node,
		api:        api,
	}
	return downloader, nil
}

func NewRpcIpfsDownloader(ipfs_url string) (downloader *IpfsDownloader, err error) {
	api, err := rpc.NewURLApiWithClient(ipfs_url, http.DefaultClient)
	if err != nil {
		return nil, err
	}
	return &IpfsDownloader{
		api: api,
	}, nil
}

func (d *IpfsDownloader) GetFile(ctx context.Context, id string) (string, error) {
	var prepared_id string
	if id[:7] == "ipfs://" {
		prepared_id = "/ipfs/" + id[7:]
	} else if id[:6] == "/ipfs/" {
		prepared_id = id
	} else {
		return "", fmt.Errorf("invalid ipfs url")
	}
	newPath, err := path.NewPath(prepared_id)
	if err != nil {
		return "", err
	}
	n, err := d.api.Unixfs().Get(ctx, newPath)
	if err != nil {
		return "", err
	}
	file, ok := n.(files.File)

	if !ok {
		return "", fmt.Errorf("not a file")
	}
	size, err := file.Size()
	if err != nil {
		return "", err
	}
	if size > MAX_IPFS_FILESIZE {
		return "", fmt.Errorf("file is too big")
	}
	var buf bytes.Buffer
	_, err = io.Copy(&buf, file)
	if err != nil {
		return "", err
	}

	data := buf.Bytes()
	return string(data), nil
}

func (d *IpfsDownloader) Close() {
	if d.node != nil {
		d.node.Close()
	}
}
