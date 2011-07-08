package main

import "github.com/abneptis/riak"
import (
	"http"
	"log"
	"flag"
	"sync"
)

var flag_threads = flag.Int("threads", 1, "Number of delete-threads")
var flag_verbose = flag.Bool("verbose", false, "Verbose delete")


func main(){
	flag.Parse()
	c,_  := riak.NewClient("", http.URL{Scheme:"http", Host: "localhost:8098", Path:"/"})
	wg := &sync.WaitGroup{}
	args := flag.Args()
	if len(args) == 0 {
		log.Fatalf("Usage: clean_bucket BUCKETNAME [...]")
	}

	queue := [][2]string{}

	for bi := range(args) {
			ch := make(chan string, 32)
			go func(b string){
				for i := range(ch) {
					queue = append(queue, [2]string{b, i}) 
				}
			}(args[bi])
			err := riak.ListKeys(c, flag.Arg(0), ch, nil)
			if err != nil{
				log.Printf("Couldn't get bucket: %v", err)
				return
			}
	}
	thchan := make(chan int, *flag_threads)
	for i := 0; i < *flag_threads; i ++ { thchan <- i }

	for i := range(queue){
		wg.Add(1)
		if *flag_verbose {
			log.Printf("Calling delete on %s - %s", queue[i][0], queue[i][1])
		}
		go func(b, k string){
			<- thchan
			err := riak.DeleteItem(c, b, k, nil, nil)
			thchan <- 1
			if err != nil {
				log.Printf("Error deleting %s - %s: %v", b,k,err)
			}
			wg.Done()
		}(queue[i][0], queue[i][1])
	}
	log.Printf("Waiting on children..") 
	wg.Wait()
}
