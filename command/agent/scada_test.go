package agent

import (
	"net"
	"reflect"
	"testing"

	"github.com/hashicorp/scada-client"
)

func TestProviderService(t *testing.T) {
	conf := DefaultConfig()
	conf.Version = "0.5.0"
	conf.VersionPrerelease = "rc1"
	conf.Atlas = &AtlasConfig{}
	conf.Atlas.Join = true
	conf.Server.Enabled = true
	ps := ProviderService(conf)

	expect := &client.ProviderService{
		Service:        "nomad",
		ServiceVersion: "0.5.0rc1",
		Capabilities: map[string]int{
			"http": 1,
		},
		Meta: map[string]string{
			"auto-join":  "true",
			"region":     "global",
			"datacenter": "dc1",
			"client":     "false",
			"server":     "true",
		},
		ResourceType: "nomad-cluster",
	}

	if !reflect.DeepEqual(ps, expect) {
		t.Fatalf("bad: %v", ps)
	}
}

func TestProviderConfig(t *testing.T) {
	conf := DefaultConfig()
	conf.Version = "0.5.0"
	conf.VersionPrerelease = "rc1"
	conf.Atlas = &AtlasConfig{}
	conf.Atlas.Join = true
	conf.Atlas.Infrastructure = "armon/test"
	conf.Atlas.Token = "foobarbaz"
	conf.Atlas.Endpoint = "foo.bar:1111"
	conf.Server.Enabled = true
	pc := ProviderConfig(conf)

	expect := &client.ProviderConfig{
		Service: &client.ProviderService{
			Service:        "nomad",
			ServiceVersion: "0.5.0rc1",
			Capabilities: map[string]int{
				"http": 1,
			},
			Meta: map[string]string{
				"auto-join":  "true",
				"region":     "global",
				"datacenter": "dc1",
				"client":     "false",
				"server":     "true",
			},
			ResourceType: "nomad-cluster",
		},
		Handlers: map[string]client.CapabilityProvider{
			"http": nil,
		},
		Endpoint:      "foo.bar:1111",
		ResourceGroup: "armon/test",
		Token:         "foobarbaz",
	}

	if !reflect.DeepEqual(pc, expect) {
		t.Fatalf("bad: %v", pc)
	}
}

func TestSCADAListener(t *testing.T) {
	list := newScadaListener("armon/test")
	defer list.Close()

	var raw interface{} = list
	_, ok := raw.(net.Listener)
	if !ok {
		t.Fatalf("bad")
	}

	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	go list.Push(a)
	out, err := list.Accept()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != a {
		t.Fatalf("bad")
	}
}

func TestSCADAAddr(t *testing.T) {
	var addr interface{} = &scadaAddr{"armon/test"}
	_, ok := addr.(net.Addr)
	if !ok {
		t.Fatalf("bad")
	}
}
