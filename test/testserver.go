package main

import(
	"os"
	"RPC-golang/rpcserver"
)
func main(){

	//read the config file
	configFilePath := os.Args[1]
	configObject := &rpcserver.ConfigType{}
	if err:=configObject.ReadConfig(configFilePath);err!=nil{
		return
	}
	//Initialize server with the config object
	rpcServer := &rpcserver.RPCServer{}
	rpcServer.InitializeServer(*configObject)

	//create the server and listen to incoming connections
	if err:=rpcServer.CreateServer();err!=nil{
		return
	}
	
}
