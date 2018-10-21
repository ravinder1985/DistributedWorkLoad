package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type httpInterface interface {
	httpServerStart(httpConnection *http.Server)
	commandHandler(w http.ResponseWriter, r *http.Request)
}
type Response struct {
	Status string `json:"status"`
	Code   int    `json:"code"`
}

type Work struct {
	Operation string `json:"operation"`
	Topic     string `json:"topic"`
}

type ClientManager struct {
	clients     map[*Client]bool
	register    chan *Client
	unregister  chan *Client
	work        chan []byte
	httpRequest chan []byte
}

type Client struct {
	socket  net.Conn
	address net.Addr
	data    chan []byte
	load    int
}

func (manager *ClientManager) validOperations(message []byte) {
	fmt.Println("msg received from http: " + string(message))
	ReceiveWork := Work{}
	if err := json.Unmarshal(message, &ReceiveWork); err != nil {
		//fmt.Println("Msg is not json string", client.address)
		fmt.Println(err)
	} else {
		switch ReceiveWork.Operation {
		case "kafka_topic_process":
			//fmt.Println("kafka_topic_process", client.address)
			fmt.Println(ReceiveWork.Topic)
			manager.work <- []byte("kafka_topic_process/r/n")
			break
		case "union":
			//fmt.Println("union operation", client.address)
			fmt.Println(ReceiveWork.Topic)
			manager.work <- []byte("union/r/n")
			fmt.Println("sent to manager.work")
			break
		case "info":
			for connection, status := range manager.clients {
				if ok := status; ok {
					fmt.Println(connection.address, status)
				}
			}
			break
		default:
			fmt.Println("Unknown command")
		}
		fmt.Println(ReceiveWork)
	}
}

func (manager *ClientManager) start() {
	for {
		select {
		case connection := <-manager.register:
			manager.clients[connection] = true
			fmt.Println("New worker joined")
		case connection := <-manager.unregister:
			if _, ok := manager.clients[connection]; ok {
				close(connection.data)
				delete(manager.clients, connection)
				fmt.Println("worker disconnected")
			}
		case getHttpRequest := <-manager.httpRequest:
			fmt.Println("in getHttpRequest")
			go manager.validOperations(getHttpRequest)
		case getWorkDone := <-manager.work:
			fmt.Println("getWorkDone")
			for connection, status := range manager.clients {
				if ok := status; ok {
					connection.load++
					fmt.Println("sending work to :", connection.address)
					connection.data <- getWorkDone
					if connection.load >= 10 {
						manager.clients[connection] = false
					}
					break
				}
			}
		}
	}
}

func (manager *ClientManager) receive(client *Client) {
	for {
		message := make([]byte, 1024)
		length, err := client.socket.Read(message)
		if err != nil {
			fmt.Println("Could not read msg from ", client.address)
			manager.unregister <- client
			client.socket.Close()
			break
		}
		m := bytes.Split(message[:length], []byte("/r/n"))
		for i := 0; i < len(m); i++ {
			commands := m[i]
			if len(m[i]) != 0 {
				fmt.Println("msg received from client: " + string(commands))
				ReceiveWork := Work{}

				if err := json.Unmarshal(commands, &ReceiveWork); err != nil {
					//fmt.Println("Msg is not json string", client.address)
					fmt.Println(err)
				} else {
					switch ReceiveWork.Operation {
					case "join":
						fmt.Println("############## Join event ##############")
						break
					case "kafka_topic_process":
						//fmt.Println("kafka_topic_process", client.address)
						fmt.Println(ReceiveWork.Topic)
						manager.work <- []byte("kafka_topic_process/r/n")
						break
					case "union":
						//fmt.Println("union operation", client.address)
						fmt.Println(ReceiveWork.Topic)
						manager.work <- []byte("union/r/n")
						fmt.Println("sent union/r/n to manager.work")
						break
					case "info":
						for connection, status := range manager.clients {
							if ok := status; ok {
								fmt.Println(connection.address, status)
							}
						}
						break
					case "done":
						// Work completed
						client.load--
						fmt.Println(client.address, client.load)
						if client.load < 10 {
							manager.clients[client] = true
						}
					default:
						fmt.Println("Unknown command")
					}
					fmt.Println(ReceiveWork)
				}
			}
		}
	}
}

func handleWork(conn net.Conn, commands string) {
	fmt.Println("msg received: " + commands)
	time.Sleep(1000 * time.Millisecond)
	fmt.Println("Work completed")
	// Response back the master after completed the work
	conn.Write([]byte(`{"operation":"done"}/r/n`))
}

func (client *Client) receive(wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	for {
		message := make([]byte, 1024)
		length, err := client.socket.Read(message)
		if err != nil {
			fmt.Println("Could not read msg")
			client.socket.Close()
			break
		}

		if length > 0 {
			m := bytes.Split(message[:length], []byte("/r/n"))

			for i := 0; i < len(m); i++ {
				commands := string(m[i])
				if len(m[i]) != 0 {
					// Swith case would be here to handle perticular worker
					// After completed return Done to the master
					go handleWork(client.socket, commands)
				}
			}
		}
	}
}

func (manager *ClientManager) send(client *Client) {
	for {
		select {
		case message, ok := <-client.data:
			fmt.Println(ok)
			if !ok {
				return
			}
			client.socket.Write(message)
		}
	}
}
func (client *Client) commandHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println("validation failed")
	}
	var validWorkStruct Work
	err = json.Unmarshal(body, &validWorkStruct)
	if err != nil {
		fmt.Println("validation failed")
	}
	// Send it to master to make decision to send this work
	body = append(body, '/', 'r', '/', 'n')
	client.socket.Write(body)
	fmt.Println(validWorkStruct)
	w.Header().Set("Content-Type", "application/json")
	response := Response{"success", 200}
	json.NewEncoder(w).Encode(response)
}

func (manager *ClientManager) commandHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Println("validation failed")
	}
	var validWorkStruct Work
	err = json.Unmarshal(body, &validWorkStruct)
	if err != nil {
		fmt.Println("validation failed")
	}
	manager.httpRequest <- []byte(body)

	fmt.Println(validWorkStruct)
	w.Header().Set("Content-Type", "application/json")
	response := Response{"success", 200}
	json.NewEncoder(w).Encode(response)
}

func (client *Client) httpServerStart(httpConnection *http.Server) {
	fmt.Println("Starting http server in client...")
	if err := httpConnection.ListenAndServe(); err != nil {
		fmt.Println("Something is wrong", err)
	}
	fmt.Println("Shutdown http server in client...")
	return
}
func (manager *ClientManager) httpServerStart(httpConnection *http.Server) {
	fmt.Println("Starting http server in manager...")
	if err := httpConnection.ListenAndServe(); err != nil {
		fmt.Println("Something is wrong", err)
	}
	fmt.Println("Shutdown http server in manager...")
	return
}

func StartHttpServer(server httpInterface, port string) *http.Server {
	port = ":" + port
	httpConnection := &http.Server{Addr: port}
	go server.httpServerStart(httpConnection)
	return httpConnection
}

func StartManager(port *string) {
	fmt.Println("Starting Server...")
	l, e := net.Listen("tcp", ":9090")
	if e != nil {
		fmt.Println(e)
	}
	manager := ClientManager{
		clients:     make(map[*Client]bool),
		register:    make(chan *Client),
		unregister:  make(chan *Client),
		work:        make(chan []byte),
		httpRequest: make(chan []byte),
	}

	go manager.start()

	http.HandleFunc("/input", manager.commandHandler)
	httpConnection := StartHttpServer(&manager, *port)
	fmt.Println(httpConnection.Addr)
	for {
		connection, error := l.Accept()
		if error != nil {
			fmt.Println(error)
		}
		client := &Client{socket: connection, address: connection.RemoteAddr(), data: make(chan []byte), load: 0}
		manager.register <- client
		go manager.receive(client)
		go manager.send(client)
	}

}

func StartClient(name *string, port *string, c chan bool, count *int, client *Client) {
	wg := new(sync.WaitGroup)
	fmt.Println("Starting client...")
	connection, error := net.Dial("tcp", "localhost:9090")
	if error != nil {
		fmt.Println(error)
		time.Sleep(5000 * time.Millisecond)
		c <- true
		return
	}
	client.socket = connection
	//client := &Client{socket: connection}
	go client.receive(wg)
	if *count <= 0 {
		*count = *count + 1
		http.HandleFunc("/input", client.commandHandler)
	}
	httpConnection := StartHttpServer(client, *port)
	fmt.Println(httpConnection.Addr)
	connection.Write([]byte(`{"operation":"join", "topic":" ` + *name + `"}/r/n`))
	time.Sleep(2000 * time.Millisecond)
	wg.Wait()
	fmt.Println("Disconnected...")
	fmt.Println("ShutDown http connection...")
	if err := httpConnection.Shutdown(nil); err != nil {
		panic(err)
	}
	time.Sleep(2000 * time.Millisecond)
	fmt.Println("Retry Dialing to server...")
	c <- true
	// reader := bufio.NewReader(os.Stdin)
	// message, _ := reader.ReadString('\n')
	// connection.Write([]byte(strings.TrimRight(message, "\n")))
}

func main() {

	flagMode := flag.String("mode", "server", "start in client or server mode")
	port := flag.String("port", "8080", "start in client or server mode")
	name := flag.String("name", "client", "start in client or server mode")
	count := 0
	flag.Parse()
	if strings.ToLower(*flagMode) == "server" {
		StartManager(port)
	} else {
		c := make(chan bool, 1)
		client := &Client{}
		c <- true
		for {
			select {
			case t := <-c:
				if t == true {
					StartClient(name, port, c, &count, client)
				}
			}
		}
	}
}

// func main() {
// 	listen, err := net.Listen("tcp4", ":9999")
// 	defer listen.Close()
// 	if err != nil {
// 		log.Fatalf("Socket listen port %d failed,%s", 9999, err)
// 		os.Exit(1)
// 	}
// 	log.Printf("Begin listen port: %d", 9999)
//
// 	for {
// 		conn, err := listen.Accept()
// 		if err != nil {
// 			log.Fatalln(err)
// 			continue
// 		}
// 		defer conn.Close()
//
// 		var (
// 			buf = make([]byte, 1024)
// 			r   = bufio.NewReader(conn)
// 			w   = bufio.NewWriter(conn)
// 		)
// 		n, err := r.Read(buf)
// 		data := string(buf[:n])
// 		switch data {
// 		case "PING":
// 			log.Println("Receive: PING command")
// 		case "LS":
// 			log.Println("Receive: LS command")
// 		default:
// 			log.Println("Unknown command")
// 		}
// 		log.Println("Receive:", data)
// 		w.Write([]byte("PONG"))
// 		w.Flush()
// 		log.Printf("Send: %s", "PONG")
//
// 	}
// }
