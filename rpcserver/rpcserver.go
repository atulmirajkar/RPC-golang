package rpcserver

import (
	"fmt"
	"encoding/json"
	"io/ioutil"
	"net/rpc"
	"net/rpc/jsonrpc"
	"net"
	"strconv"
	"log"
	"github.com/boltdb/bolt"
	"errors"
	"sync"
	"os"
	//"time"
)


type Dict3 struct{
	Key string
	Relation string
	Value interface{}

}

type RequestParameters struct{
	Method string `json:"method,omitempty"`
	Params json.RawMessage `json: "params"`
	Id int `json" "id,omitempty"` 
}

type ResponseParameters struct{
	Result []interface{} `json:"result"`
	Id int `json: "id,omitempty"`
	Error interface{} `json:"error"`
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

//this struct object will manage the server
type RPCServer struct{
	configObject ConfigType 
	boltDB * bolt.DB
	stopChan  chan int
	wg * sync.WaitGroup
	wgLock *sync.Mutex
	logger * log.Logger
	logFile os.File
}

//this struct methods will be exposed to client
type RPCMethod struct{
	rpcServer *RPCServer
}



var rpcServerInstance *RPCServer = nil
func init(){
	
	rpcServerInstance = &RPCServer{}
	fmt.Println("Server Instance Created")
}

func GetRPCServerInstance() (error,*RPCServer){
	if rpcServerInstance==nil{
		err := errors.New("Server Instance not created succesfully")
		return err,nil
	}

	return nil,rpcServerInstance
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

	return nil
}
/*****************************Memory Mapped Persitent FIle Operations using Bolt starts*******************************/
func (rpcMethod *RPCMethod) insertOrUpdate(reqPar json.RawMessage) error{
//Unmarshal into array of interfaces
	var parameters []interface{}
	if err :=json.Unmarshal(reqPar, &parameters); err!=nil{
		rpcMethod.rpcServer.logger.Println(err)
		return err
	}
		
	//Use dict3 struct to unmarshall
	dict3:=Dict3{} 
	for k,v:=range parameters{
		rpcMethod.rpcServer.logger.Println(k,v)
		if k==0 { 
			dict3.Key = v.(string)
		} else if k==1{
			dict3.Relation = v.(string)
		} else if k==2{
			dict3.Value=v
		}
	}

	//Marshal the value and store in db
	valueByte,err :=json.Marshal(dict3.Value)
	if  err!=nil{
		rpcMethod.rpcServer.logger.Println(err)
		return err
	}


	//open db in update mode - insert or update

	rpcMethod.rpcServer.boltDB.Update(func(tx *bolt.Tx) error {
		
		b, err := tx.CreateBucketIfNotExists([]byte(dict3.Key))
		if err != nil {
			return err
		}
		
		b = tx.Bucket([]byte(dict3.Key))
		if err = b.Put([]byte(dict3.Relation), valueByte); err!=nil{
			return err
		}
		return nil
	})
	
	
	
	
	return nil
	
}
func (rpcMethod *RPCMethod) insert(reqPar json.RawMessage, response *ResponseParameters) (error){
	
	//Unmarshal into array of interfaces
	var parameters []interface{}
	if err :=json.Unmarshal(reqPar, &parameters); err!=nil{
		rpcMethod.rpcServer.logger.Println(err)
		return err
	}
		
	//Use dict3 struct to unmarshall
	dict3:=Dict3{} 
	for k,v:=range parameters{
		rpcMethod.rpcServer.logger.Println(k,v)
		if k==0 { 
			dict3.Key = v.(string)
		} else if k==1{
			dict3.Relation = v.(string)
		} else if k==2{
			dict3.Value=v
		}
	}

	//Marshal the value and store in db
	valueByte,err :=json.Marshal(dict3.Value)
	if  err!=nil{
		rpcMethod.rpcServer.logger.Println(err)
		return err
	}

	
	//open db in view mode
	//check if key already present
	//check if rel already present
	//if both present return err
	//if not open db in update mode and create
	
	var keyPresent bool
	keyPresent = false
	rpcMethod.rpcServer.boltDB.View(func(tx *bolt.Tx) error {
		//check if key present
		b := tx.Bucket([]byte(dict3.Key))
		if b!=nil{
			v :=b.Get([]byte(dict3.Relation))
			if v!=nil{
				keyPresent = true
			}
		}
		return nil
	})


	//open db in update mode
	if !keyPresent{
		rpcMethod.rpcServer.boltDB.Update(func(tx *bolt.Tx) error {
		
			b, err := tx.CreateBucketIfNotExists([]byte(dict3.Key))
			if err != nil {
				return err
			}
			
			b = tx.Bucket([]byte(dict3.Key))
			if err = b.Put([]byte(dict3.Relation), valueByte); err!=nil{
				return err
			}
			return nil
		})
		//Marshall the responseparameters
		//Result []interface{} `json:"result"`
		//Id int `json: "id"`
		//Error int `json:"error"`
		response.Result = make([]interface{},1)
		response.Result[0] = "true" 
		response.Error = nil
		
	}else{
		//return an error
		//Marshall the responseparameters
		//Result []interface{} `json:"result"`
		//Id int `json: "id"`
		//Error int `json:"error"`
		
		response.Result = make([]interface{},1)
		response.Result[0] = "false" 
		response.Error = 1

	}
	
	
	
	
	return nil
}

func (rpcMethod * RPCMethod) delete(reqPar json.RawMessage) error{
	//Unmarshal into array of interfaces
	var parameters []interface{}
	if err :=json.Unmarshal(reqPar, &parameters); err!=nil{
		rpcMethod.rpcServer.logger.Println(err)
		return err
	}
		
	//Use dict3 struct to unmarshall
	dict3:=Dict3{} 
	// Key string
	// Relation string
	// Value interface{}

	for k,v:=range parameters{
		rpcMethod.rpcServer.logger.Println(k,v)
		if k==0 { 
			dict3.Key = v.(string)
		} else if k==1{
			dict3.Relation = v.(string)
		} 
	}

	//Read value from db
	var keyPresent bool
	keyPresent = false
	var dict3Value []byte
	var bucket *(bolt.Bucket)
	rpcMethod.rpcServer.boltDB.View(func(tx *bolt.Tx) error{
		bucket= tx.Bucket([]byte(dict3.Key))
		if bucket!=nil{
			dict3Value=bucket.Get([]byte(dict3.Relation))
			if dict3Value!=nil{
				keyPresent = true
				
			}
		}
		
		return nil
	})
	
	
	rpcMethod.rpcServer.logger.Println(bucket)
	
	//1. get bucket
	//2. delete relation
	//3. delete if bucket empty - delete bucket
	if keyPresent{
		rpcMethod.rpcServer.boltDB.Update(func(tx * bolt.Tx)error{

			bucket= tx.Bucket([]byte(dict3.Key))
			bucket.Delete([]byte(dict3.Relation))
			var bucketStats bolt.BucketStats 
			bucketStats = tx.Bucket([]byte(dict3.Key)).Stats()
			
			//if bucket empty delete bucket
			if bucketStats.KeyN == 0{
				tx.DeleteBucket([]byte(dict3.Key))
			}
			return nil	
		})
		
	
	}

	return nil
}


//its the same as list buckets
func (rpcMethod * RPCMethod) listKeys(response *ResponseParameters) error{
	
	//open a read transaction
	rpcMethod.rpcServer.boltDB.View(func (tx *bolt.Tx) error{
		var cursor *bolt.Cursor
		cursor = tx.Cursor()
		
		//append to reselt the list of buckets
		response.Result = make([]interface{},0,10)
		for k,_ := cursor.First(); k!=nil ; k,_= cursor.Next(){
			rpcMethod.rpcServer.logger.Println("BUCKET ",string(k))
			response.Result =append(response.Result,string(k))
		}
	
		return nil
	})

	response.Error = nil

	return nil

}

func (rpcMethod * RPCMethod) listIDs(response * ResponseParameters) error{

	//open a read transaction
	rpcMethod.rpcServer.boltDB.View(func (tx *bolt.Tx) error{
		var cursor *bolt.Cursor
		cursor = tx.Cursor()

		var bucket *bolt.Bucket
		response.Result = make([]interface{},0,10)

		//traverse through all keys
		for k,_ := cursor.First();k!=nil;k,_=cursor.Next(){
			bucket = tx.Bucket(k)
			

			//traverse through all relation and value pairs
			bucket.ForEach(func (relation, value []byte) error{
				tuple := make([]string,2)
				rpcMethod.rpcServer.logger.Println(string(k),string(relation), string(value))
				//make an array of 2 strings [key,relation]
				tuple[0] = string(k)
				tuple[1] = string(relation)
				response.Result = append(response.Result,tuple)
				return nil
			})
		}
		return nil
	})

	response.Error = nil
	return nil

}

func (rpcMethod * RPCMethod) lookup(reqPar json.RawMessage, response *ResponseParameters) error{
	//Unmarshal into array of interfaces
	var parameters []interface{}
	if err :=json.Unmarshal(reqPar, &parameters); err!=nil{
		rpcMethod.rpcServer.logger.Println(err)
		return err
	}
		
	//Use dict3 struct to unmarshall
	dict3:=Dict3{} 
	// Key string
	// Relation string
	// Value interface{}

	for k,v:=range parameters{
		rpcMethod.rpcServer.logger.Println(k,v)
		if k==0 { 
			dict3.Key = v.(string)
		} else if k==1{
			dict3.Relation = v.(string)
		} 
	}

	//Read value from db
	var keyPresent bool
	keyPresent = false
	var dict3Value []byte

	rpcMethod.rpcServer.boltDB.View(func(tx *bolt.Tx) error{
		b:= tx.Bucket([]byte(dict3.Key))
		if b!=nil{
			dict3Value=b.Get([]byte(dict3.Relation))
			if dict3Value!=nil{
				keyPresent = true
				
			}
		}

		return nil
	})
	rpcMethod.rpcServer.logger.Println(dict3Value, keyPresent)
	
	//if key present unmarshall 
	if keyPresent{
		//unmarshall in interface - second argument for unmarshall is a pointer
		if err :=json.Unmarshal(dict3Value,&(dict3.Value));err!=nil{
		
			rpcMethod.rpcServer.logger.Println("Value Unmarshalling error ",err," for id: ",dict3.Key," ",dict3.Relation)
			//if error send error
			//response.Result = make([]interface{},1)
			//response.Result[0] = "false" 
			response.Result = nil
			response.Error = "Unmarshalling Error"
 
		}
		//save unmarhslled in dict3 Result and Error
		response.Result = make([]interface{},3)
		response.Result[0] = dict3.Key 
		response.Result[1] = dict3.Relation
		response.Result[2] = dict3.Value
		response.Error = nil

 		
	}else{
		//if key value not found return false
		rpcMethod.rpcServer.logger.Println("Value not found: ",dict3.Key," ",dict3.Relation)
		//response.Result = make([]interface{},1)
		//response.Result[0] = "false" 
		//response.Error = nil
		response.Result = nil
		response.Error = "Value not found"
		
	}

	return nil
}

func (rpcMethod * RPCMethod) shutDown() error {
	rpcMethod.rpcServer.logger.Println(&(rpcMethod.rpcServer.stopChan))
	rpcMethod.rpcServer.logger.Print(*(rpcMethod.rpcServer.wg))
	rpcMethod.rpcServer.logger.Println(" in shutdown")
	
	rpcMethod.rpcServer.stopChan <- 1
	return nil
}
/*****************************Memory Mapped Persitent FIle Operations using Bolt Ends*******************************/

/*****************************Exposed Wrappers to actual methods start**********************************************/

//wrapper to insert
func (rpcMethod * RPCMethod) Insert(jsonInput []byte,jsonOutput *[]byte) error{
	//Initialize rpcserver
	var err error
	err,rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err!=nil{
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}
	
	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()
	

	defer rpcMethod.rpcServer.routineDone()
	//Unmarshall the request in RequestParameters
	var reqPar RequestParameters
	/*
	Method string `json:"Method"`
	Params json.RawMessage `json: "params"`
	Id int 'json" "id"' 
        */
	if err :=json.Unmarshal(jsonInput, &reqPar); err!=nil{
		customError= errors.New("Message request unmarshalling error:" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
		
	}
	rpcMethod.rpcServer.logger.Println(reqPar.Method)

	response:=new(ResponseParameters)

	if err :=rpcMethod.insert(reqPar.Params,response); err!=nil{
		rpcMethod.rpcServer.logger.Println(err)
		response.Result = make([]interface{},1)
		response.Result[0] = err
		response.Error = 1
	}
	//just set ID over here
	//the rest response is set by respective method 
	//inserOrUpdate / delete / shutdown does not return anything
	if response !=nil{
		response.Id = reqPar.Id
		
		//var jsonOutputTemp []byte
		var err error
		if (*jsonOutput), err = json.Marshal(response); err!=nil{
			rpcMethod.rpcServer.logger.Println(err)
			return err
		}
		rpcMethod.rpcServer.logger.Println(string(*jsonOutput))
	}else {
		(*jsonOutput),_ = json.Marshal(&ResponseParameters{Result: nil, Id: -1, Error : nil})
	}
	
	
	
	return nil

	
}

//wrapper to insertorupdate
func (rpcMethod * RPCMethod) InsertOrUpdate(jsonInput []byte,jsonOutput *[]byte) error{
//Initialize rpcserver
	var err error
	err,rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err!=nil{
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}
	
	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()
	

	defer rpcMethod.rpcServer.routineDone()
	//Unmarshall the request in RequestParameters
	var reqPar RequestParameters
	/*
	Method string `json:"Method"`
	Params json.RawMessage `json: "params"`
	Id int 'json" "id"' 
        */
	if err :=json.Unmarshal(jsonInput, &reqPar); err!=nil{
		customError= errors.New("Message request unmarshalling error:" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
		
	}
	rpcMethod.rpcServer.logger.Println(reqPar.Method)

	response:=new(ResponseParameters)

	if err :=rpcMethod.insertOrUpdate(reqPar.Params); err!=nil{
		//even though error, we are not returning anything
		rpcMethod.rpcServer.logger.Println(err)
	}
	response = nil
	


	//just set ID over here
	//the rest response is set by respective method 
	//inserOrUpdate / delete / shutdown does not return anything
	if response !=nil{
		response.Id = reqPar.Id
		
		//var jsonOutputTemp []byte
		var err error
		if (*jsonOutput), err = json.Marshal(response); err!=nil{
			rpcMethod.rpcServer.logger.Println(err)
			return err
		}
		rpcMethod.rpcServer.logger.Println(string(*jsonOutput))
	}else {
		(*jsonOutput),_ = json.Marshal(&ResponseParameters{Result: nil, Id: -1, Error : nil})
	}
	
	
	
	return nil

}


//wrapper to delete
func (rpcMethod * RPCMethod) Delete(jsonInput []byte,jsonOutput *[]byte) error{
	//Initialize rpcserver
	var err error
	err,rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err!=nil{
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}
	
	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()
	

	defer rpcMethod.rpcServer.routineDone()
	//Unmarshall the request in RequestParameters
	var reqPar RequestParameters
	/*
	Method string `json:"Method"`
	Params json.RawMessage `json: "params"`
	Id int 'json" "id"' 
        */
	if err :=json.Unmarshal(jsonInput, &reqPar); err!=nil{
		customError= errors.New("Message request unmarshalling error:" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
		
	}
	rpcMethod.rpcServer.logger.Println(reqPar.Method)

	response:=new(ResponseParameters)

	if err :=rpcMethod.delete(reqPar.Params); err!=nil{
		rpcMethod.rpcServer.logger.Println(err)
		response.Result[0] = err
		response.Error = 1
		//return err
	}

	//just set ID over here
	//the rest response is set by respective method 
	//inserOrUpdate / delete / shutdown does not return anything
	if response !=nil{
		response.Id = reqPar.Id
		
		//var jsonOutputTemp []byte
		var err error
		if (*jsonOutput), err = json.Marshal(response); err!=nil{
			rpcMethod.rpcServer.logger.Println(err)
			return err
		}
		rpcMethod.rpcServer.logger.Println(string(*jsonOutput))
	}else {
		(*jsonOutput),_ = json.Marshal(&ResponseParameters{Result: nil, Id: -1, Error : nil})
	}
	
	
	
	return nil

}


//wrapper to shutdown
func (rpcMethod * RPCMethod) Shutdown(jsonInput []byte,jsonOutput *[]byte) error{
	//Initialize rpcserver
	var err error
	err,rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err!=nil{
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}
	
	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()
	

	defer rpcMethod.rpcServer.routineDone()
	//Unmarshall the request in RequestParameters
	var reqPar RequestParameters
	/*
	Method string `json:"Method"`
	Params json.RawMessage `json: "params"`
	Id int 'json" "id"' 
        */
	if err :=json.Unmarshal(jsonInput, &reqPar); err!=nil{
		customError= errors.New("Message request unmarshalling error:" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
		
	}
	rpcMethod.rpcServer.logger.Println(reqPar.Method)

	response:=new(ResponseParameters)
	
	if err :=rpcMethod.shutDown(); err!=nil{
		response.Result = make([]interface{},1)
		response.Result[0] = err
		response.Error = 1
		
		//return err
	}
	
	//just set ID over here
	//the rest response is set by respective method 
	//inserOrUpdate / delete / shutdown does not return anything
	if response !=nil{
		response.Id = reqPar.Id
		
		//var jsonOutputTemp []byte
		var err error
		if (*jsonOutput), err = json.Marshal(response); err!=nil{
			rpcMethod.rpcServer.logger.Println(err)
			return err
		}
		rpcMethod.rpcServer.logger.Println(string(*jsonOutput))
	}else {
		(*jsonOutput),_ = json.Marshal(&ResponseParameters{Result: nil, Id: -1, Error : nil})
	}
	
	
	
	return nil

}

//wrapper to listkeys
func (rpcMethod * RPCMethod) ListKeys(jsonInput []byte,jsonOutput *[]byte) error{
	//Initialize rpcserver
	var err error
	err,rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err!=nil{
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}
	
	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()
	

	defer rpcMethod.rpcServer.routineDone()
	//Unmarshall the request in RequestParameters
	var reqPar RequestParameters
	/*
	Method string `json:"Method"`
	Params json.RawMessage `json: "params"`
	Id int 'json" "id"' 
        */
	if err :=json.Unmarshal(jsonInput, &reqPar); err!=nil{
		customError= errors.New("Message request unmarshalling error:" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
		
	}
	

	response:=new(ResponseParameters)

	if err :=rpcMethod.listKeys(response); err!=nil{
		rpcMethod.rpcServer.logger.Println(err)
		response.Result[0] = err
		response.Error = 1
	}

	//test
	//time.Sleep(100 * time.Millisecond)
	//test
	//just set ID over here
	//the rest response is set by respective method 
	//inserOrUpdate / delete / shutdown does not return anything
	if response !=nil{
		response.Id = reqPar.Id
		
		//var jsonOutputTemp []byte
		var err error
		if (*jsonOutput), err = json.Marshal(response); err!=nil{
			rpcMethod.rpcServer.logger.Println(err)
			return err
		}
		rpcMethod.rpcServer.logger.Println(string(*jsonOutput))
	}else {
		(*jsonOutput),_ = json.Marshal(&ResponseParameters{Result: nil, Id: -1, Error : nil})
	}
	
	
	
	return nil

}


//wrapper to listIDs
func (rpcMethod * RPCMethod) ListIDs(jsonInput []byte,jsonOutput *[]byte) error{
	//Initialize rpcserver
	var err error
	err,rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err!=nil{
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}
	
	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()
	

	defer rpcMethod.rpcServer.routineDone()
	//Unmarshall the request in RequestParameters
	var reqPar RequestParameters
	/*
	Method string `json:"Method"`
	Params json.RawMessage `json: "params"`
	Id int 'json" "id"' 
        */
	if err :=json.Unmarshal(jsonInput, &reqPar); err!=nil{
		customError= errors.New("Message request unmarshalling error:" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
		
	}
	rpcMethod.rpcServer.logger.Println(reqPar.Method)

	response:=new(ResponseParameters)

	if err :=rpcMethod.listIDs(response); err!=nil{
		rpcMethod.rpcServer.logger.Println(err)
		response.Result[0] = err
		response.Error = 1
	}

	
	//just set ID over here
	//the rest response is set by respective method 
	//inserOrUpdate / delete / shutdown does not return anything
	if response !=nil{
		response.Id = reqPar.Id
		
		//var jsonOutputTemp []byte
		var err error
		if (*jsonOutput), err = json.Marshal(response); err!=nil{
			rpcMethod.rpcServer.logger.Println(err)
			return err
		}
		rpcMethod.rpcServer.logger.Println(string(*jsonOutput))
	}else {
		(*jsonOutput),_ = json.Marshal(&ResponseParameters{Result: nil, Id: -1, Error : nil})
	}
	
	
	
	return nil

}

//wrapper to lookup
func (rpcMethod * RPCMethod) Lookup(jsonInput []byte,jsonOutput *[]byte) error{
	//Initialize rpcserver
	var err error
	err,rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err!=nil{
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}
	
	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()
	

	defer rpcMethod.rpcServer.routineDone()
	//Unmarshall the request in RequestParameters
	var reqPar RequestParameters
	/*
	Method string `json:"Method"`
	Params json.RawMessage `json: "params"`
	Id int 'json" "id"' 
        */
	if err :=json.Unmarshal(jsonInput, &reqPar); err!=nil{
		customError= errors.New("Message request unmarshalling error:" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
		
	}
	rpcMethod.rpcServer.logger.Println(reqPar.Method)

	response:=new(ResponseParameters)

	if err :=rpcMethod.lookup(reqPar.Params,response); err!=nil{
		rpcMethod.rpcServer.logger.Println(err)
		response.Result[0] = err
			response.Error = 1
		//return err
	}
	
	//just set ID over here
	//the rest response is set by respective method 
	//inserOrUpdate / delete / shutdown does not return anything
	if response !=nil{
		response.Id = reqPar.Id
		
		//var jsonOutputTemp []byte
		var err error
		if (*jsonOutput), err = json.Marshal(response); err!=nil{
			rpcMethod.rpcServer.logger.Println(err)
			return err
		}
		rpcMethod.rpcServer.logger.Println(string(*jsonOutput))
	}else {
		(*jsonOutput),_ = json.Marshal(&ResponseParameters{Result: nil, Id: -1, Error : nil})
	}
	
	
	
	return nil

}
/*****************************Exposed Wrappers to actual methods Ends**********************************************/


// /*
// create multiple methods:
// each method will receive the whole the whole json string
// strip the string and check if the method param is same
// */
// func (rpcMethod *RPCMethod) DICT3Service(jsonInput []byte,jsonOutput *[]byte) error{
	
// 	//Initialize rpcserver
// 	var err error
// 	err,rpcMethod.rpcServer = GetRPCServerInstance()
// 	var customError error
// 	if err!=nil{
// 		customError = errors.New("Getting Server Instance error :" + err.Error())
// 		fmt.Println(customError)
// 		return customError
// 	}
	
// 	rpcMethod.rpcServer.wgLock.Lock()
// 	rpcMethod.rpcServer.wg.Add(1)
// 	rpcMethod.rpcServer.wgLock.Unlock()
	

// 	defer rpcMethod.rpcServer.routineDone()
// 	//Unmarshall the request in RequestParameters
// 	var reqPar RequestParameters
// 	/*
// 	Method string `json:"Method"`
// 	Params json.RawMessage `json: "params"`
// 	Id int 'json" "id"' 
//         */
// 	if err :=json.Unmarshal(jsonInput, &reqPar); err!=nil{
// 		customError= errors.New("Message request unmarshalling error:" + err.Error())
// 		fmt.Println(customError)
// 		return customError
		
// 	}
// 	fmt.Println(reqPar.Method)

// 	response:=new(ResponseParameters)

// 	//do matching of methods in config	
// 	switch reqPar.Method{
//  	case "insert": 	
// 		if err :=rpcMethod.insert(reqPar.Params,response); err!=nil{
// 			fmt.Println(err)
// 			response.Result = make([]interface{},1)
// 			response.Result[0] = err
// 			response.Error = 1
// 			//return err
// 		}
// 	case "shutdown":  
// 		if err :=rpcMethod.shutDown(); err!=nil{
// 			response.Result = make([]interface{},1)
// 			response.Result[0] = err
// 			response.Error = 1
			
// 			//return err
// 		}
		
// 		//make response nil
// 		response = nil
// 	case "lookup":
// 		if err :=rpcMethod.lookup(reqPar.Params,response); err!=nil{
// 			fmt.Println(err)
// 			response.Result[0] = err
// 			response.Error = 1
// 			//return err
// 		}
// 	case "insertOrUpdate":
// 		if err :=rpcMethod.insertOrUpdate(reqPar.Params); err!=nil{
// 			//even though error, we are not returning anything
// 			fmt.Println(err)
// 		}
// 		response = nil

		
// 	case "delete":
// 		if err :=rpcMethod.delete(reqPar.Params); err!=nil{
// 			fmt.Println(err)
// 			response.Result[0] = err
// 			response.Error = 1
// 			//return err
// 		}
// 	case "listKeys":
// 		if err :=rpcMethod.listKeys(response); err!=nil{
// 			fmt.Println(err)
// 			response.Result[0] = err
// 			response.Error = 1
// 		}
// 	case "listIDs":
// 		if err :=rpcMethod.listIDs(response); err!=nil{
// 			fmt.Println(err)
// 			response.Result[0] = err
// 			response.Error = 1
// 		}

// 	default:
// 		customError = errors.New("Request Message could not be understood")
// 		fmt.Println(customError)
// 		return customError

// 	} 
	
// 	//just set ID over here
// 	//the rest response is set by respective method 
// 	//inserOrUpdate / delete / shutdown does not return anything
// 	if response !=nil{
// 		response.Id = reqPar.Id
		
// 		//var jsonOutputTemp []byte
// 		var err error
// 		if (*jsonOutput), err = json.Marshal(response); err!=nil{
// 			fmt.Println(err)
// 			return err
// 		}
// 		fmt.Println(string(*jsonOutput))
// 	}else {
// 		(*jsonOutput),_ = json.Marshal(&ResponseParameters{Result: nil, Id: 0, Error : 0})
// 	}
	
	
	
// 	return nil
// }

/*****************************Server Helper Routines start**********************************************/


func (rpcServer *RPCServer)routineDone(){
	rpcServer.wgLock.Lock()
	rpcServer.wg.Done()
	rpcServer.wgLock.Unlock()

}

func (rpcServer *RPCServer)InitializeServerConfig(inputConfigObject ConfigType) error {


	//initialize config
	rpcServer.configObject =  inputConfigObject

	//intialize db
	var err error
	rpcServer.boltDB, err = bolt.Open(rpcServer.configObject.PersistentInfo.PersistentFilePath, 0600, nil)
	if err != nil {
		log.Fatal(err)
	} 
	
	//initialize channel
	//sender gets blocked gets 
	//rpcServer.stopChan = make(chan int)
	//to make single sender unblocking 
	rpcServer.stopChan = make(chan int,1)
	fmt.Println("Initialized Config to Server")
	
	rpcServer.wg = &sync.WaitGroup{}
	rpcServer.wgLock = &sync.Mutex{}

	//intialize logger
	file,e := os.Create("logger.txt")
	if e!=nil{
	 	fmt.Println("File Error: %v\n", e)
		return e
	}

	rpcServer.logger = log.New(file,"log: ",log.LstdFlags)

	return nil
}

func (rpcServer *RPCServer)closeServerAndDB(listener net.Listener)error{
	<-rpcServer.stopChan

	
	(rpcServer.wg).Wait()
	

	
	rpcServer.logger.Println("Closing Connection")
	listener.Close()

	//close logger
	rpcServer.logFile.Close()

	
	//once all connections are served close db and return
	rpcServer.boltDB.Close()
	fmt.Println("Server Connection closing")
	fmt.Println("DB Connection closing")
	var err error
	err = errors.New("Stop Server")	
	return err

}
func (rpcServer *RPCServer)CreateServer() error{

	//register method
	rpcServer.logger.Println("In createserver")
	
	rpcServer.logger.Println(rpcServer.configObject.ServerID)
	if err :=rpc.RegisterName(rpcServer.configObject.ServerID,new(RPCMethod)); err!=nil{
		rpcServer.logger.Println(err)
		return err
	
	}
	
	
	//will use http protocol
	rpc.HandleHTTP()
	rpcServer.logger.Println(rpcServer.configObject.Protocol,":" + strconv.Itoa(rpcServer.configObject.Port))
	//listen on port
	listener, err := net.Listen(rpcServer.configObject.Protocol, ":" + strconv.Itoa(rpcServer.configObject.Port))
	if err!=nil {
		rpcServer.logger.Println(err)
		return err
	}
	//asynchronously start a methd and listen on channel
	go rpcServer.closeServerAndDB(listener)
	//infinite for to listen requests
	for{


		conn,err := listener.Accept()
		if err!=nil{
			rpcServer.logger.Println(err)
			return err
		}


		go jsonrpc.ServeConn(conn)

		
	}
	
	return nil
}


/*****************************Server Helper Routines Ends**********************************************/


