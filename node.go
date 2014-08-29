package main

import (
    "bufio"
    "io"
    "flag"
    "fmt"
    "log"
    "math/rand"
    "net"
    "strconv"
    "strings"
    "time"
)

const (
    LEADER = iota
    CANDIDATE
    FOLLOWER
)

type Addr struct {
    Host string
    Port int
    Addr string
}
type RaftServer struct {
    Role          int
    Nodes         []Addr
    Port          int
    HeartbeatChan chan bool
    ElectChan     chan bool
    IsElecting    bool
    ETO           int `election time out`
    Votes         int `total vote for me`
    NoVote        int `current No. Vote`
    HasVote       bool
}

func (rs *RaftServer) Heartbeat() {
    for {
        <-rs.HeartbeatChan
        log.Println("Leader starts Heartbeat")
        for _, n := range rs.Nodes {
            if n.Port == rs.Port {
                continue
            }
            c, err := net.Dial("tcp", n.Addr)
            if err != nil {
                log.Println(err)
            } else {
                fmt.Fprintf(c, "LEADER-HEARTBEAT-SYNC\n")
                line, err := bufio.NewReader(c).ReadString('\n')
                if err != nil && err != io.EOF{
                    log.Println(err)
                } else {
                    log.Println("Heartbeat Reply:" + strings.TrimRight(line,"\n"))
                }
                c.Close()
            }
        }
    }
}
func (rs *RaftServer) Elect() {
    for {
        log.Println("wait to elect")
        <-rs.ElectChan
        //clear the votes
        rs.Votes = 0
        rs.NoVote += 1
        rs.IsElecting = true
        log.Println("start elect:vote" + strconv.Itoa(rs.NoVote))
        for _, n := range rs.Nodes {
            if n.Port == rs.Port {
                continue
            }
            c, err := net.Dial("tcp", n.Addr)
            if err != nil {
                log.Println(err)
            } else {
                fmt.Fprintf(c, "NoVote"+strconv.Itoa(rs.NoVote)+"\n")
                line, err := bufio.NewReader(c).ReadString('\n')
                if err != nil {
                    log.Println(err)
                } else {
                    log.Println("Elect Reply:" + strings.TrimRight(line,"\n"))
                    if strings.HasPrefix(line, "OK") {
                        log.Println("Receive Vote from:", c)
                        rs.Votes += 1
                    }
                }
                c.Close()
            }
        }

        if (rs.Votes+1)*2 > len(rs.Nodes) {
            rs.ChangeRole(LEADER)
        }
        rs.IsElecting = false
    }
}
func (rs *RaftServer) ResetETO() {
    rs.ETO = 1500 + rand.Intn(1500)
}
func (rs *RaftServer) ChangeRole(role int) {
    switch role{
        case LEADER:
            log.Println("I becomes the Leader!")
        case CANDIDATE:
            log.Println("I becomes the candidate!")
        case FOLLOWER:
            log.Println("I becomes a follower!")
    }
    rs.Role = role
}
func (rs *RaftServer) EelectTimer() {
    for {
        for rs.ETO != 0{
            oldeto := rs.ETO
            rs.ETO = 0
            time.Sleep(time.Duration(oldeto) * time.Millisecond)
        }

        if rs.ETO == 0 {
            log.Println("EelectTimer timed out!")
            if rs.Role != LEADER && !rs.IsElecting{
                rs.ElectChan <- true
                rs.ChangeRole(CANDIDATE)
            }
        }
        rs.ResetETO()
    }
}
func (rs *RaftServer) HeartbeatTimer() {
    for {
        if rs.Role == LEADER {
            rs.HeartbeatChan <- true
        }
        time.Sleep(500 * time.Millisecond)
    }
}
func (rs *RaftServer) CanVote(vote string) bool {
    noVote, _ := strconv.Atoi(strings.Replace(vote, "NoVote", "", -1))
    if rs.Role == LEADER {
        if noVote > rs.NoVote {
            rs.NoVote = noVote
        }
        return false
    }

    if noVote > rs.NoVote {
        rs.NoVote = noVote
        rs.HasVote = true
        return true
    } else if noVote == rs.NoVote {
        if !rs.HasVote {
            rs.HasVote = true
            return true
        }
    }
    return false
}
func (rs *RaftServer) Run() {
    ln, err := net.Listen("tcp", ":"+strconv.Itoa(rs.Port))
    if err != nil {
        log.Fatal(err)
    }
    log.Println("Listening as " + strconv.Itoa(rs.Port))
    go rs.Elect()
    go rs.Heartbeat()
    go rs.EelectTimer()
    go rs.HeartbeatTimer()

    for {
        conn, err := ln.Accept()
        if err != nil {
            // handle error
            continue
        }
        go func(c net.Conn) {
            // Echo all incoming data.
            line, err := bufio.NewReader(c).ReadString('\n')
            if err != nil {
                log.Println(err)
                c.Close()
                return
            } else {
                log.Println("Server Receive:" + strings.TrimRight(line,"\n"))
                if strings.HasPrefix(line,"LEADER-HEARTBEAT-SYNC") {
                    if rs.Role != FOLLOWER{
                        rs.ChangeRole(FOLLOWER)
                    }
                    rs.ResetETO()
                    fmt.Fprintf(c, "FOLLOWER ACK\n")
                } else if strings.HasPrefix(line, "NoVote") {
                    if rs.CanVote(line) {
                        fmt.Fprintf(c, "OK\n")
                    } else {
                        fmt.Fprintf(c, "NOTOK\n")
                    }
                }
            }
            //io.Copy(c, c)
            // Shut down the connection.
            c.Close()
        }(conn)
    }
}

func main() {
    log.SetFlags(23)
    port := flag.Int("p", 5000, "listening port")
    flag.Parse()

    rs := RaftServer{}
    rs.ChangeRole(FOLLOWER)
    rs.ElectChan = make(chan bool)
    rs.HeartbeatChan = make(chan bool)
    rs.ResetETO()
    rs.Nodes = []Addr{
        {"127.0.0.1", 5000, ":5000"},
        {"127.0.0.1", 5001, ":5001"},
        {"127.0.0.1", 5002, ":5002"},
    }
    rs.Port = *port
    rs.Run()
}
