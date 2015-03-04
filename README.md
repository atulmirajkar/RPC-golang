# RPC-golang
RPC using golang
Use of Golang's  built in concurrency functionality to create a Remote Procedure Call  

make server
Installs the user server packages and third party library “BOLT”. Uses $GOPATH for
installation.

make client
Installs the user client packages. Uses $GOPATH for installation.
make
Install both client and the server on the same machine

make clean
Cleans the installed packages and the third party library. Also cleans the executable created. Uses
$GOBIN.

Run Server:
testserver server_config_file

Run Client
testclient server_config_file < file_message_buffer
