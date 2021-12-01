package main

import ("flag"
	"fmt"
	"net"
	"pairbroker/stubs"
	"net/rpc")

var ( mulch = make(chan int, 2))

//This is just a helper function that attempts to determine this
//process' IP address.
func getOutboundIP() string {
	conn, _ := net.Dial("udp", "8.8.8.8:80")
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr).IP.String()
	return localAddr
}

func makedivision(ch chan int, client *rpc.Client){
	for {
		x:= <-ch
		y:= <-ch
		newpair := stubs.Pair{X:x, Y:y}
		towork := stubs.PublishRequest{Topic: "divide", Pair:newpair}
		status := new(stubs.StatusReport)
		err := client.Call(stubs.Publish, towork, status)
		if err != nil {
			fmt.Println("RPC client returned error:")
			fmt.Println(err)
			fmt.Println("Dropping division.")
		}
	}
}

type Factory struct {}

func (f *Factory) Multiply(req stubs.Pair, res *stubs.JobReport) (err error) {
	res.Result = req.X * req.Y
	fmt.Println(req.X, "*", req.Y, "=", res.Result)
	mulch <- res.Result
	return
}

func (f *Factory) Divide(req stubs.Pair, res *stubs.JobReport) (err error) {
	res.Result = req.X / req.Y
	fmt.Println(req.X, "/", req.Y, "=", res.Result)
	return
}

func main(){
	pAddr := flag.String("port","8050","Port to listen on")
	brokerAddr := flag.String("broker","127.0.0.1:8030", "Address of broker instance")
	flag.Parse()
	client, _ := rpc.Dial("tcp", *brokerAddr)
	status := new(stubs.StatusReport)
	client.Call(stubs.CreateChannel, stubs.ChannelRequest{Topic: "multiply", Buffer: 10}, status)
	client.Call(stubs.CreateChannel, stubs.ChannelRequest{Topic: "divide", Buffer: 10}, status)
	rpc.Register(&Factory{})
	fmt.Println(*pAddr)
	listener, err := net.Listen("tcp", ":"+*pAddr)
	if err != nil {
		fmt.Println(err)
	}
	client.Call(stubs.Subscribe, stubs.Subscription{Topic: "multiply", FactoryAddress: getOutboundIP()+":"+*pAddr, Callback: "Factory.Multiply"}, status)
	client.Call(stubs.Subscribe, stubs.Subscription{Topic: "divide", FactoryAddress: getOutboundIP()+":"+*pAddr, Callback: "Factory.Divide"}, status)
	defer listener.Close()
	go makedivision(mulch, client)
	rpc.Accept(listener)
	flag.Parse()
}
