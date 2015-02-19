package rpcserver

import (
	"fmt"
	"encoding/json"
	"io/ioutil"
	"net/rpc"
	"net/rpc/jsonrpc"
	//"log"
	//"net/http"
	"net"
	"strconv"
)



type Dict3 struct{
	Key string  
	Relation string
	Value interface{}
}

type RequestParameters struct{
	Method string `json:"Method"`
	Params json.RawMessage `json: "params"`
	Id int `json:"id"`
}

type ResponseParameters struct{
	Result []interface{} `json:"result"`
	Id int `json: "id"`
	Error int `json:"error"`
}


//can be a file or a database
//Here using a database
type PersistentContainerType struct{
	PersistentFilePath string `json:"file"`
}

type ConfigType struct{
	ServerID string `json:"serverID"`
	Protocol string `json:"protocol"`
	IpAddress string `json: "ipAddress"`
	Port int `json: "port"`
	PersistentInfo PersistentContainerType `json: "persistentStorageContainer"`
	Methods []string `json: "methods"`
	
}

type RPCServer struct{
	configObject ConfigType 

}

type RPCMethod struct{

}

func (configObject *ConfigType)ReadConfig(configFilePath string) error{

	file,e := ioutil.ReadFile(configFilePath)
	if e!=nil{
	 	fmt.Println("File Error: %v\n", e)
		return e
	}
	//Unmarshall the json file
	if e:=json.Unmarshal(file, configObject); e!=nil{
		fmt.Println(e)
		return e
	}
	fmt.Printf("Results: %v\n", configObject)
	return nil
}


/*
create multiple methods:
each method will receive the whole the whole json string
strip the string and check if the method param is same
*/
func (server *RPCMethod) Insert(jsonInput []byte,jsonOutput *[]byte) error{
	var reqPar RequestParameters
	if err :=json.Unmarshal(jsonInput, &reqPar); err!=nil{
		fmt.Println(err)
		return err

	}
	//*jsonOutput = reqPar.Id

	fmt.Println(reqPar.Method)
	var parameters []interface{}
	if err :=json.Unmarshal(reqPar.Params, &parameters); err!=nil{
		fmt.Println(err)
		return err
	}
	
	for k,v:=range parameters{
		fmt.Println(k,v)
	}
	fmt.Println(reqPar.Id)
	
	response:=new(ResponseParameters)
	response.Result = make([]interface{},1)
	response.Result[0] = reqPar.Method 
	response.Id = reqPar.Id
	response.Error = 0
	//var jsonOutputTemp []byte
	(*jsonOutput),_ = json.Marshal(response)
	fmt.Println(string(*jsonOutput))
	
	return nil
}


func (rpcServer *RPCServer)InitializeServer(inputConfigObject ConfigType){
	rpcServer.configObject =  inputConfigObject
}

func (rpcServer *RPCServer)CreateServer() error{

	if err :=rpc.Register(new(RPCMethod)); err!=nil{
		fmt.Println(err)
		return err
	
	}
	rpc.HandleHTTP()
	fmt.Println(rpcServer.configObject.Protocol,":" + strconv.Itoa(rpcServer.configObject.Port))
	listener, err := net.Listen(rpcServer.configObject.Protocol, ":" + strconv.Itoa(rpcServer.configObject.Port))
	if err!=nil {
		fmt.Println(err)
		return err
	}
	for{
		conn,err := listener.Accept()
		if err!=nil{
			fmt.Println(err)
			continue
		}
		go jsonrpc.ServeConn(conn)
	}


}


