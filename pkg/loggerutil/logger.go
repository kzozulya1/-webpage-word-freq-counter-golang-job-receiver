//Package loggerutil
package loggerutil

import (
	"log"
	"os"
)

//Do log -
func Log(msg interface{}, filename string) {
	f, _ := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	defer f.Close()
	log.SetOutput(f)
	switch msg.(type) {
		case string:
			log.Printf("%s",msg)				
		default:
			log.Printf("%#v",msg)
	}
}
