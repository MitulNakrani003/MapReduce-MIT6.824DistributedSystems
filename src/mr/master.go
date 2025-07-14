package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"


type TaskStatus int
const (
    Idle TaskStatus = iota
    InProgress
    Completed
)

type Task struct {
    Status    TaskStatus
    StartTime time.Time
}

type Master struct {
    mu         sync.Mutex
    mapTasks   []Task
    reduceTasks []Task
    files      []string
    nReduce    int
    done       bool
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(args *TaskRequestArgs, reply *TaskRequestReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	//Assign map tasks first
    for i, task := range m.mapTasks {
        if task.Status == Idle {
            
            m.mapTasks[i].Status = InProgress
            m.mapTasks[i].StartTime = time.Now()
            
            reply.TaskType = MapTask
            reply.TaskID = i
            reply.Filename = m.files[i]
            reply.NReduce = m.nReduce

            return nil
        }
    }

	//If maps not done, tell worker to wait
    for _, task := range m.mapTasks {
        if task.Status != Completed {
            reply.TaskType = WaitTask
            return nil
        }
    }
    
    // Assign reduce tasks
    for i, task := range m.reduceTasks {
        if task.Status == Idle {
            m.reduceTasks[i].Status = InProgress
            m.reduceTasks[i].StartTime = time.Now()

            reply.TaskType = ReduceTask
            reply.TaskID = i
            reply.ReduceID = i

            return nil
        }
    }
    
    // All tasks completed
    reply.TaskType = ExitTask
    m.done = true
    return nil

}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
        files:      files,
        nReduce:    nReduce,
        mapTasks:   make([]Task, len(files)),
        reduceTasks: make([]Task, nReduce),
    }
    for i := range m.mapTasks {
        m.mapTasks[i].Status = Idle
    }
    for i := range m.reduceTasks {
        m.reduceTasks[i].Status = Idle
    }

    m.server()

    return &m
}
