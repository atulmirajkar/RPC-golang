package rpcclient

import(
	"fmt"
	"net/rpc/jsonrpc"
	"encoding/json"
	"io/ioutil"
	"time"
	"net/rpc"
	"errors"
	"os"

)

//structure for parsing Response from server
type ResponseParameters struct{
	Result json.RawMessage `json:"result"`
	Id int `json: "id,omitempty"`
	Error interface{} `json:"error"`
}

type ConsoleParameters struct{
	Result []interface{} `json:"result"`
	Id int `json: "id,omitempty"`
	Error interface{} `json:"error"`

}
//structure for getting the request name

type RequestParameters struct{
	Method string `json:"Method"`
	Params json.RawMessage `json: "params"`
	Id int `json" "id"` 
}

//structure for parsing config file
type ConfigType struct{
	ServerID string `json:"serverID"`
	Protocol string `json:"protocol"`
	IpAddress string `json: "ipAddress"`
	Port int `json: "port"`
	Methods []string `json: "methods"`
	
}


//read config file
func (configObject *ConfigType)ReadConfig(configFilePath string) error{
	file,e := ioutil.ReadFile(configFilePath)
	if e!=nil{
	 	fmt.Printf("File Error: %v\n", e)
		return e
	}

	if e = json.Unmarshal(file, configObject); e!=nil{
		fmt.Printf("JSON Marshalling Error: %v\n", e)
		return e
	
	}

	return nil;
	
}

//Structure for RPC Client
type RPCClient struct{
	//the client connection
	connection *rpc.Client
	//number of simultaneous channels
	numChannels int 
	//a channel containing pointers to simultaneous RPC calls
	doneChan chan *rpc.Call
	
	//add timeout as well
	
} 

//RPCClient creator
func (client *RPCClient)NewClient(network string, address string, numChannels int) error{
	conn, err := jsonrpc.Dial(network, address)
	client.connection = conn
	if err!=nil {
		fmt.Println(err)
		return err
	}
	//create a buffered channel for buffering simultaneous calls 
	client.doneChan =make(chan *rpc.Call,numChannels)

	
	return nil
}

func extractMethodName(byteRequest []byte,rpcFunction *string) (error){
	/*
	Method string `json:"Method"`
	Params json.RawMessage `json: "params"`
	Id int 'json" "id"' 
        */
	
	var reqPar RequestParameters
	if err :=json.Unmarshal(byteRequest, &reqPar); err!=nil{
		customError:= errors.New("Message request unmarshalling error:" + err.Error())
		fmt.Println(customError)
		return customError
		
	}
	//check if it ins in the list of methd names
	
	(*rpcFunction) = string(reqPar.Method[0]-'a' + 65) + reqPar.Method[1:] 

	return  nil

}
//create Asynchronous RPC calls
func (client * RPCClient)CreateAsyncRPC(jsonMessages []string, serverName string) error{
	var byteRequest []byte
	var response []byte
	
	//directly send jsonmessages to the server asynchronously
	var rpcFunction string
	//test
	// byteRequest = []byte(jsonMessages[0])
	// if err :=extractMethodName(byteRequest,&rpcFunction); err!=nil{
	// 	fmt.Println(err)
	// 	return err
	// }
	// rpcServerAndFunction := serverName + "." + rpcFunction
		
	// replyCall :=<-client.connection.Go(rpcServerAndFunction,byteRequest,&response,nil).Done
	// client.doneChan = replyCall.Done
	//test
	for _,request :=range jsonMessages {
	//for _,request :=range jsonMessages[1:] {	
		//fmt.Println(request)
		byteRequest = []byte(request)
		if err :=extractMethodName(byteRequest,&rpcFunction); err!=nil{
			fmt.Println(err)
			return err
		}
		rpcServerAndFunction := serverName + "." + rpcFunction
		replyCall :=client.connection.Go(rpcServerAndFunction,byteRequest,&response,client.doneChan)

		if replyCall.Error!=nil{
			return replyCall.Error 
		}
	}
	return nil
	
}

//process calls by reading the channel of Calls
func (client *RPCClient)ProcessReplies(numRequests int)error{
	
	//should take timeout as config argument
	var timeout <-chan time.Time
	timeout = time.After(10000 * time.Millisecond)
	for i:=0; i<(numRequests);i++ {
		select{
			//case when channel has got a call object 
		case replyCall := <- client.doneChan:
			//fmt.Println(string((replyCall.Args).([]byte)))
			
		
			if replyCall.Error !=nil{
				fmt.Println(replyCall.Error)
				return replyCall.Error 
			}
			//unmarshall the response parameter
			parameters := new(ResponseParameters)
			if err:=json.Unmarshal(*(replyCall.Reply).(*[]byte),parameters);err!=nil{
				fmt.Println(err)
				return err
			
			}

			
			//the result parameter is returned as array of interface and parsed as raw json in the above
			//call. Unmarshall this raw json into array of interfaces
			var resultParameter []interface{}
			if err:=json.Unmarshal((*parameters).Result, &resultParameter); err!=nil{
				fmt.Println(err)
				return err
			}
			
			//this array of interfaces has to be parsed according to the type of request 
			// for eg. insert or lookup
			//for k,v := range resultParameter{
			//	fmt.Println(k,v)
			//}			
			
			//fmt.Println(*parameters)
			//fmt.Println(string((*parameters).Result))


			//fmt.Println("Using Encoder")
			var cp = ConsoleParameters{}
			cp.Result= resultParameter
			cp.Id = parameters.Id
			cp.Error = parameters.Error
			encoder := json.NewEncoder(os.Stdout)
			encoder.Encode(cp)
			//initialize timout
			timeout = time.After(10000 * time.Millisecond)
		case <-timeout:
			fmt.Println("Timed Out")
	
		}
		
	}
	
	return nil
}
