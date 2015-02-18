package main

import(
	"RPC-golang/rpcclient"
	"fmt"
	"os"
	"strconv"
)

func main(){

	//unmarshal config file in configObject
	configFilePath := os.Args[1]
	configObject := &rpcclient.ConfigType{}
	if err:=configObject.ReadConfig(configFilePath); err!=nil{
		fmt.Println("Error reading Config file",err)
		return
	}
	

	//create the connection
	//number of simultaneous channels should be taken from config file
	numChannels := 10
	network := configObject.Protocol
	address := configObject.IpAddress + ":" + strconv.Itoa(configObject.Port)
	client := &rpcclient.RPCClient{}
	
	//create new client
	if err :=client.NewClient(network,address,numChannels); err!=nil{
		fmt.Println(err)
		return
	
	} 
	
	//make asychronous calls
	if err := client.CreateAsyncRPC(os.Args[2:]); err!=nil{
		fmt.Println(err)
		return
	}
	
	//process replies from server
	if err := client.ProcessReplies(len(os.Args)-2); err!=nil{
		fmt.Println(err)
		return
	}
	
	
}
