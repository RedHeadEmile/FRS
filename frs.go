package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	connHost = "0.0.0.0"
	connPort = "4514"
	connType = "tcp"

	millis15 = time.Second * 15
)

var connections = make(map[string]net.Conn)

func skip(i int, array []string) string {
	var rs string
	alen := len(array)
	for k, v := range array {
		if k >= i {
			rs += v
			if i != alen-1 {
				rs += " "
			}
		}
	}
	return rs
}

func write(key, field, value string) {
	if key == "null" || key == "" {
		return
	}
	if value == "null" {
		os.Remove(filepath.FromSlash("./data/" + key + "/" + field))
	} else {
		os.MkdirAll(filepath.FromSlash("./data/" + key), 0777)
		ioutil.WriteFile(filepath.FromSlash("./data/" + key + "/" + field), []byte(value), 0777)
	}
}

func get(key, field string) string {
	if key == "" || key == "null" {
		return ""
	}
	data, err := ioutil.ReadFile(filepath.FromSlash("./data/" + key + "/" + field))
	if err != nil {
		fmt.Println("Error: " + err.Error())
		return "null"
	}
	return string(data)
}

func getAll(key string) []string {
	files, err := filepath.Glob(filepath.FromSlash("./data/" + key + "/*"))
	if err != nil {
		fmt.Println("Error: " + err.Error())
		return []string{}
	}
	var filesName = make([]string, len(files))
	i := 0
	for _, v := range files {
		paths := strings.Split(v, filepath.FromSlash("/"))
		filesName[i] = paths[len(paths)-1]
		i++
	}
	return filesName
}

func main() {
	// Listen
	l, err := net.Listen(connType, connHost+":"+connPort)
	if err != nil {
		fmt.Println("Error listening: " + err.Error())
		os.Exit(1)
	}
	// Close when the application closes
	defer l.Close()
	fmt.Println("Listening on " + connHost + ":" + connPort)
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connection in goroutine
		go handleRequest(conn)
		time.Sleep(time.Millisecond * 2)
	}
}

func handleRequest(conn net.Conn) {
	var key = conn.RemoteAddr().String()
	fmt.Println("New connection " + key)
	connections[key] = conn
	defer func() {
		delete(connections, key)
		conn.Close()
		fmt.Println("Lost " + key)
	}()
	buffer := bufio.NewReader(conn)
	for {
		conn.SetReadDeadline(time.Now().Add(millis15))
		conn.SetWriteDeadline(time.Now().Add(millis15))

		bytes, err := buffer.ReadBytes('\n')
		if err != nil {
			return
		}

		msg := string(bytes)
		for _, value := range strings.Split(msg, "\n") {
			if value == "0 areyouhere" {
				conn.Write([]byte("imhere\n"))
			} else {
				data := strings.Split(value, " ")
				identifier, key := data[0], func() string {
					if len(data) > 2 {
						if data[2] == "null" {
							return ""
						}
						return data[2]
					}
					return ""
				}()

				if len(data) <= 1 || key == "" {
					continue
				}

				if data[1] == "get" {
					if identifier != "0" && len(data) > 3 {
						conn.Write([]byte("rep " + identifier + " " + get(key, data[3]) + "\n"))
					}
				} else if data[1] == "getall" {
					if identifier != "0" {
						conn.Write([]byte("rep " + identifier + " " + skip(0, getAll(key)) + "\n"))
					}
				} else if data[1] == "set" && len(data) > 3 {
					go write(key, data[3], func() string {
						value := skip(4, data)
						if value != "" {
							return value
						}
						return "null"
					}())
					if identifier != "0" {
						conn.Write([]byte("rep 0\n"))
					}
				} else if data[1] == "publish" {
					fmt.Println("publish " + key + " " + skip(3, data) + "\n")
					go func() {
						for keyMap, connection := range connections {
							_, err := connection.Write([]byte("publish " + key + " " + skip(3, data) + "\n"))
							if err != nil {
								delete(connections, keyMap)
							}
						}
					}()
					if identifier != "0" {
						conn.Write([]byte("rep 0\n"))
					}
				}
			}
		}
	}
}
