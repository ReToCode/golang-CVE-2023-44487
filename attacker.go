package main

import (
	"bytes"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

var numRequests int
var numConnections int
var concurrencyLimit int
var serverURLStr string
var waitTime int
var delayTime int

type connection struct {
	streamCounter uint32
	sentHeaders   int32
	sentRSTs      int32
	recvFrames    int32
	framer        *http2.Framer
}

// Accept command line arguments
func init() {
	flag.IntVar(&numConnections, "connections", 1, "Number of connections to open")
	flag.IntVar(&numRequests, "requests", 5, "Number of requests to send for each connection")
	flag.StringVar(&serverURLStr, "url", "https://localhost:8443", "Server URL")
	flag.IntVar(&waitTime, "wait", 0, "Wait time in milliseconds between starting workers")
	flag.IntVar(&delayTime, "delay", 0, "Delay in milliseconds between sending HEADERS and RST_STREAM")
	flag.IntVar(&concurrencyLimit, "concurrency", 1, "Maximum number of concurrent worker routines for each connection")
	flag.Parse()
}

func main() {

	serverURL, err := url.Parse(serverURLStr)
	if err != nil {
		log.Fatalf("Failed to parse URL: %v", err)
	}

	path := serverURL.Path
	if path == "" {
		path = "/"
	}

	headerStart := time.Now()
	connections := []*connection{}
	var connWg sync.WaitGroup
	for i := 0; i < numConnections; i++ {
		con := &connection{
			streamCounter: 1,
			sentHeaders:   0,
			sentRSTs:      0,
			recvFrames:    0,
		}
		connWg.Add(1)
		go con.Start(serverURL, path, func() {
			connWg.Done()
		})
		connections = append(connections, con)
	}
	connWg.Wait()

	headerEnd := time.Now()

	elapsed := headerEnd.Sub(headerStart).Seconds()
	fmt.Printf("\n--- Summary ---\n")

	headerSum := int32(0)
	for i, c := range connections {
		fmt.Printf("Conn: %d - Frames sent: HEADERS = %d, RST_STREAM = %d\n", i, c.sentHeaders, c.sentRSTs)
		fmt.Printf("Conn: %d - Frames received: %d\n", i, c.recvFrames)
		headerSum += c.sentHeaders
	}
	fmt.Printf("Total time: %.2f seconds (%d rps)\n\n", elapsed, int(math.Round(float64(headerSum)/elapsed)))
}

func (c *connection) Start(serverURL *url.URL, path string, doneFunc func()) {
	defer doneFunc()

	// Establish TLS with server and ignore certificate
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h2"},
	}

	conn, err := tls.Dial("tcp", serverURL.Host, tlsConfig)
	if err != nil {
		log.Fatalf("Failed to dial: %s", err)
	}

	_, err = conn.Write([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"))
	if err != nil {
		log.Fatalf("Failed to send client preface: %s", err)
	}

	// Initialize HTTP2 framer and mutex
	c.framer = http2.NewFramer(conn, conn)
	var mu sync.Mutex

	// Send initial SETTINGS frame
	mu.Lock()
	if err := c.framer.WriteSettings(); err != nil {
		log.Fatalf("Failed to write settings: %s", err)
	}
	mu.Unlock()

	// Read and count received frames, print to stdout
	go func() {
		for {
			_, err := c.framer.ReadFrame()
			if err != nil {
				if err == io.EOF {
					fmt.Println("Received io.EOF")
					return
				}
				fmt.Printf("Failed to read frame: %s", err)
			} else {
				atomic.AddInt32(&c.recvFrames, 1)
				//fmt.Printf("Received frame: %v\n", frame)
			}
		}
	}()

	// Wait for SETTINGS frame from server
	for {
		frame, err := c.framer.ReadFrame()
		if err != nil {
			fmt.Printf("Failed to read frame: %s", err)
		}
		if _, ok := frame.(*http2.SettingsFrame); ok {
			break
		}
	}

	concurrencyChan := make(chan struct{}, concurrencyLimit)
	var wg sync.WaitGroup

	// Send requests
	for i := 0; i < numRequests; i++ {
		time.Sleep(time.Millisecond * time.Duration(waitTime))

		// only add a new request, if concurrency allows it
		concurrencyChan <- struct{}{}
		wg.Add(1)
		go c.sendRequest(path, serverURL, delayTime, func() {
			<-concurrencyChan
			wg.Done()
		})
	}

	// Wait for all requests to be finished
	wg.Wait()
}

// HPACK headers, write HEADERS to server, and send RST_STREAM
func (c *connection) sendRequest(path string, serverURL *url.URL, delay int, doneFunc func()) {
	defer doneFunc()
	var headerBlock bytes.Buffer

	// Encode headers
	encoder := hpack.NewEncoder(&headerBlock)

	encoder.WriteField(hpack.HeaderField{Name: ":method", Value: "GET"})
	encoder.WriteField(hpack.HeaderField{Name: ":path", Value: path})
	encoder.WriteField(hpack.HeaderField{Name: ":scheme", Value: "https"})
	encoder.WriteField(hpack.HeaderField{Name: ":authority", Value: serverURL.Host})

	streamID := atomic.AddUint32(&c.streamCounter, 2) // Increment streamCounter and allocate stream ID in units of two to ensure stream IDs are odd numbered per RFC 9113
	if err := c.framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      streamID,
		BlockFragment: headerBlock.Bytes(),
		EndStream:     true,
		EndHeaders:    true,
	}); err != nil {
		fmt.Printf("[%d] Failed to send HEADERS: %s", streamID, err)
	} else {
		atomic.AddInt32(&c.sentHeaders, 1)
		//fmt.Printf("[%d] Sent HEADERS on stream %d\n", streamID, streamID)
	}

	// Sleep for several ms before sending RST_STREAM
	time.Sleep(time.Millisecond * time.Duration(delay))

	if err := c.framer.WriteRSTStream(streamID, http2.ErrCodeCancel); err != nil {
		fmt.Printf("[%d] Failed to send RST_STREAM: %s", streamID, err)
	} else {
		atomic.AddInt32(&c.sentRSTs, 1)
		//fmt.Printf("[%d] Sent RST_STREAM on stream %d\n", streamID, streamID)
	}
}
