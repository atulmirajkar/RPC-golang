package rpcclient

import(
	"fmt"
	"net/rpc/jsonrpc"
	"encoding/json"
	"io/ioutil"
	"time"
	"net/rpc"
)

//structure for parsing Response from server
type ResponseParameters struct{
	Result json.RawMessage `json:"result"`
	Id int `json: "id"`
	Error int `json:"error"`
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

//create Asynchronous RPC calls
func (client * RPCClient)CreateAsyncRPC(jsonMessages []string) error{
	var byteRequest []byte
	var response []byte
	//directly send jsonmessages to the server asynchronously
	for _,request :=range jsonMessages {
		fmt.Println(request)
		byteRequest = []byte(request)
		//The name of the class should be in config file
		replyCall :=client.connection.Go("RPCMethod.Insert",byteRequest,&response,client.doneChan)
		if replyCall.Error!=nil{
			return replyCall.Error 
		}
	}
	return nil
	
}

//process calls by reading the channel of Calls
func (client *RPCClient)ProcessReplies(numRequests int)error{
	
	//should take timeout as config argument
	timeout:= time.After(100 * time.Millisecond)
	for i:=0; i<(numRequests);i++ {
		select{
			//case when channel has got a call object 
		case replyCall := <- client.doneChan:
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
			for k,v := range resultParameter{
				fmt.Println(k,v)
			}
			
			
			fmt.Println(*parameters)
			fmt.Println(string((*parameters).Result))
		
		case <-timeout:
			fmt.Println("Timed Out")
	
		}
		
	}
	
	return nil
}
