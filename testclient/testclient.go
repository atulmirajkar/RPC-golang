package main

import(
	"github.com/atulmirajkar/RPC-golang/rpcclient"
	"fmt"
	"os"
	"strconv"
	"bufio"
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
	serverName := configObject.ServerID
	client := &rpcclient.RPCClient{}
	
	//create new client
	if err :=client.NewClient(network,address,numChannels); err!=nil{
		fmt.Println(err)
		return
	
	} 
	jsonMessages := make([]string,0,10)
	//use bufio to readlines from file
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		jsonMessages = append(jsonMessages,scanner.Text())
	}
	
	numMessages :=len(jsonMessages)
	//make asychronous calls
	if err := client.CreateAsyncRPC(jsonMessages[0:], serverName); err!=nil{
		fmt.Println(err)
		return
	}
	
	//process replies from server
	if err := client.ProcessReplies(numMessages); err!=nil{
		fmt.Println(err)
		return
	}
	
	
}
