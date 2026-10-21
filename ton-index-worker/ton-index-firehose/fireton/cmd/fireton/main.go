// fireton is firecore with the ton_v1 extension of substreams modules. It
// runs every app firecore runs, with the same flags, for blocks of type
// sf.ton.type.v1.Block; tier1 additionally takes
// --substreams-ton-router-addr.
package main

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	firecore "github.com/streamingfast/firehose-core"
	fhCMD "github.com/streamingfast/firehose-core/cmd"
	info "github.com/streamingfast/firehose-core/firehose/info"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/substreams/wasm"
	"go.uber.org/zap"

	"github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton/extension"
)

var zlog, _ = logging.PackageLogger("fireton", "github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton")

func main() {
	// Blocks stay opaque, as in firecore.
	firecore.UnsafeRunningFromFirecore = true
	firecore.UnsafeAllowExecutableNameToBeEmpty = true

	fhCMD.Main(&firecore.Chain[*pbbstream.Block]{
		ShortName:            "ton",
		LongName:             "TON",
		FullyQualifiedModule: "github.com/toncenter/ton-indexer/ton-index-worker/ton-index-firehose/fireton",
		Version:              version,
		BlockFactory:         func() firecore.Block { return new(pbbstream.Block) },
		ConsoleReaderFactory: firecore.NewConsoleReader,
		InfoResponseFiller:   info.DefaultInfoResponseFiller,
		Tools:                &firecore.ToolsConfig[*pbbstream.Block]{},

		RegisterExtraStartFlags: func(flags *pflag.FlagSet) {
			flags.String("substreams-ton-router-addr", "", "Address of ton-tvm-router for the ton_v1 extension. "+
				"Set on tier1 only: tier1 sends it to tier2 with every request. Empty: modules importing ton_v1 do not run")
		},
		RegisterSubstreamsExtensions: func() (wasm.WASMExtensioner, error) {
			routerAddr := viper.GetString("substreams-ton-router-addr")
			zlog.Info("ton_v1 extension", zap.String("router_addr", routerAddr))
			return extension.New(routerAddr, zlog), nil
		},
	})
}

// Version value, injected via go build `ldflags` at build time, **must** not be removed or inlined
var version = "dev"
