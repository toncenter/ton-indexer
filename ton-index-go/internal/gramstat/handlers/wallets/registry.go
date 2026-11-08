package wallets

import "encoding/base64"

// Frozen agreed registry, exported by scripts/tonstat-wallets/wallet_codes.py.
// Never derive it from the evolving detector at runtime. V5 beta has two hashes;
// preprocessed, vesting, x1000, multisig and jetton wallets are excluded.
const Registry = "fc96280e446129a605659a022311bba1d0d6bef9a03960be77c86a259dc876f9"

var codes = []string{
	"oM/CxIruFqJx8s/AtzgtgXVs7LEBfQd/qqs7tgL2how=",
	"1JAvzJ+tdGmPqONTIgpo2g3PcuMryy657gQhfBfTBiw=",
	"WHzHie/xyE9G7DeX5F/ICaFP9a4k8eDHpqmcydyQYf8=",
	"XJpeaMEI4YchoHxC+ZVr+zmtd+xtYktgxXbsiO7mUyk=",
	"/pUw0yQ4Uwg+8u8LTCkIwKv2+hwx6iQ6rKpb+MfXU/E=",
	"thBBpYp5gLlG6PueGY48kE0keZ/6NldOpCUcQaVm9YE=",
	"hNr6RJ+Ypph3ibojI1gHK8D3bcRSQAKl0JGLmnXS1Zk=",
	"ZN1UgFUixb6KnbWc6gEFzPDQh4bKeb64y3nogKjXMi0=",
	"/rX/aCDi/w2Ug+fg1iyBfYRniftK5YDIeIZtlZ2r1cA=",
	"89fKU0k97trCizgZhqhJQDy6w9LFhHea8IEGWvCsS5M=",
	"5M87L0xtamHqDytUR9JmeFsmrzY32y3u5rzRqoJvNBI=",
	"IINLe3KxEhR+Gy+0V7hOdNGjDwT3N9T2KmaOlVLSty8=",
	"2M27t58sXKpnesRQdwvgNRviHhJQSG3oXMUqoz3RZIQ=",
	"Dc7tISadZgE+lbGfu1xVpvAa2tQIN7qo5SHN46AqpGw=",
	"lJTRzI7fEvBWcaGpugmSEJbrUIEeGSTsZcPGKfu4CBI=",
	"jOtFs81LXMYOquHBO5wJI5Jnf+U2sumy2AG2Lv+TH+E=",
	"ID3U81ittJmTEpqpJcrDmRa2ig5PeNJujywraer6Vnk=",
	"EayteVWEQJDyg78ji8FEmHH3g+fMCXlAjT9IWUg+hSU=",
	"kUmuUcHkaJcQzr94MCl7Fqz7rbNjqSClN4k+f/7sp2g=",
}

func registryParameter() string {
	s := "["
	for i, c := range codes {
		b, err := base64.StdEncoding.DecodeString(c)
		if err != nil || len(b) != 32 {
			panic("invalid frozen wallet hash")
		}
		if i > 0 {
			s += ","
		}
		s += "'" + c + "'"
	}
	return s + "]"
}
