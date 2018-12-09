package main

import (
	"fmt"
	"log"
	"sync"
	"context"
	"google.golang.org/grpc"
	pb "pdu-server/protos"
	"google.golang.org/grpc/reflection"
	"net"
	"os"
	"io"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/cpu"
	"time"
)

type result struct {
	err error
	res *pb.ClientControlResponse
}

type PduCollector struct {
	nid string
	upstream pb.PduServerClient
}


func watchNodeControl(client pb.PduServerClient, nid string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &pb.NodeControlRequest{
		Id: nid,
	}
	stream, err := client.WatchNodeControl(ctx, req)
	if err != nil {
		log.Fatal(err)
	}
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			log.Printf("nid=%s: and now your watch is ended\n", nid)
			return
		}
		if err != nil {
			log.Println(err)
			return
		}
		log.Printf("nid=%s: got asd=%v\n", nid, res.Asd)
	}
}


func watchSettings(ctx context.Context, upstream pb.PduServerClient, in *pb.ClientControlRequest, c chan<- result) {
	stream, err := upstream.WatchClientControl(ctx, in)
	if err != nil {
		return
	}

	for {
		res, err := stream.Recv()
		select {
		case c <- result{err, res}:
			if err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *PduCollector) WatchClientControl(in *pb.ClientControlRequest, stream pb.PduServer_WatchClientControlServer) error {
	ctx := stream.Context()
	c := make(chan result)
	var wg sync.WaitGroup

	wg.Add(1)
	go func(upstream pb.PduServerClient) {
		defer wg.Done()
		watchSettings(ctx, upstream, in, c)
	}(s.upstream)

	log.Printf("Proxying WatchClientControl for cid:%d\n", in.Cid)
	defer log.Printf("No more proxying WatchClientControl for cid:%d\n", in.Cid)

	go func() {
		wg.Wait()
		close(c)
	}()
	for res := range c {
		if res.err != nil {
			return res.err
		}
		// log.Printf("Proxying for %s: %v", in.Id, res.res)
		if err := stream.Send(res.res); err != nil {
			return err
		}
	}
	return nil
}

func (s *PduCollector) WatchNodeControl(in *pb.NodeControlRequest, stream pb.PduServer_WatchNodeControlServer) error {
	return fmt.Errorf("Not allowed.")
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}


func (s *PduCollector) ClientHello(context context.Context, in *pb.HelloRequest) (*pb.HelloResponse, error) {
//	log.Printf("Proxying SendHello: %v", in)
	in.Nid = s.nid
	return s.upstream.ClientHello(context, in)
}

func (s *PduCollector) ClientCollect(context context.Context, in *pb.CollectRequest) (*pb.CollectResponse, error) {
//	log.Printf("Proxying SendCollect: %v", in)
	return s.upstream.ClientCollect(context, in)
}

func (s *PduCollector) NodeCollect(context context.Context, in *pb.NodeCollectRequest) (*pb.CollectResponse, error) {
	return nil, fmt.Errorf("Not allowed.")
}


func grpcMain(s *PduCollector) {
	envUpstream := getEnv("DIAL", "127.0.0.1:4000")
	envListen := getEnv("LISTEN", "tcp")
	envListenAddr := getEnv("LISTENADDR", ":4001")

	log.Printf("* Collector UPSTREAM: %s\n", envUpstream)
	log.Printf("* Collector LISTEN: %s%s\n", envListen, envListenAddr)

	// CONNECT to the pdu-server upstream.
	conn, err := grpc.Dial(envUpstream, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("* Cannot connect to the upstream (%s): %v", envUpstream, err)
	}
	s.upstream = pb.NewPduServerClient(conn)

	go func(s *PduCollector) {
		// LISTEN for the pdu-clients
		listen, err := net.Listen(envListen, envListenAddr)
		if err != nil {
			log.Fatalf("* Failed to listen: %v", err)
			return
		}

		grpcServer := grpc.NewServer()
		pb.RegisterPduServerServer(grpcServer, s)
		reflection.Register(grpcServer)
		grpcServer.Serve(listen)
	}(s)
}

func collectMetrics(upstream pb.PduServerClient, nid string) {
	var data []*pb.MeasureData

	if m, err := load.Avg(); err == nil {
		data = append(data, &pb.MeasureData{ Name: "load_1", Value: uint64(m.Load1) })
		data = append(data, &pb.MeasureData{ Name: "load_5", Value: uint64(m.Load5) })
		data = append(data, &pb.MeasureData{ Name: "load_15", Value: uint64(m.Load15) })
	}

	if m, err := mem.VirtualMemory(); err == nil {
		data = append(data, &pb.MeasureData{ Name: "vmem_total", Value: m.Total })
		data = append(data, &pb.MeasureData{ Name: "vmem_free", Value: m.Free })
		data = append(data, &pb.MeasureData{ Name: "vmem_used", Value: m.Used })
	}

	if m, err := cpu.Times(false); err == nil {
		data = append(data, &pb.MeasureData{ Name: "cpu_steal", Value: uint64(m[0].Steal) })
	}

	ctx := context.Background()
	upstream.NodeCollect(ctx, &pb.NodeCollectRequest{
		Nid: nid,
		Data: data,
	})
}


func main() {
	hostname, _ := os.Hostname()
	envNId := getEnv("NID", hostname)

	log.Printf("* Collector HOSTNAME: %s\n", hostname)
	log.Printf("* Collector NID: %s\n", envNId)

	s := &PduCollector{
		nid: envNId,
	}


	grpcMain(s)

	go func() {
		for {
			collectMetrics(s.upstream, envNId)
			time.Sleep(500 * time.Millisecond)
		}
	}()

	watchNodeControl(s.upstream, s.nid)
}
