package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

//Command represents a task to be executed
type Command struct {
	Folder   string
	Filename string
	Action   string
}

//TaskQueue is a thread-safe queue for commands
type TaskQueue struct {
	commands []Command
	mu       sync.Mutex
	active   map[string]context.CancelFunc //Tracks currently active tasks
}

func NewTaskQueue() *TaskQueue {
	return &TaskQueue{
		commands: []Command{},
		active:   make(map[string]context.CancelFunc),
	}
}

//AddCommand adds a command to the queue
func (q *TaskQueue) AddCommand(cmd Command) {
	q.mu.Lock()
	defer q.mu.Unlock()

	key := fmt.Sprintf("%s:%s", cmd.Folder, cmd.Filename)
	
	if cmd.Action == "stop" {
		logMessage(fmt.Sprintf("Stop command received: %s %s\n", cmd.Folder, cmd.Filename))
		if cancel, exists := q.active[key]; exists {
			cancel()              //Cancel the running task
			delete(q.active, key) //Remove active task if "stop" command
		}
	} else if cmd.Action == "start" {
		if _, exists := q.active[key]; !exists {
			logMessage(fmt.Sprintf("Start command received: %s %s\n", cmd.Folder, cmd.Filename))
			q.commands = append(q.commands, cmd)
		} else {
			logMessage(fmt.Sprintf("Task %s %s is already running. Skipping addition. \n", cmd.Folder, cmd.Filename))
		}
	}
}

//GetCommand retrieves and removes a command from the queue
func (q *TaskQueue) GetCommand() (Command, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.commands) == 0 {
		return Command{}, false
	}

	cmd := q.commands[0]
	q.commands = q.commands[1:]

	return cmd, true
}

//SetActive sets a task as active
func (q *TaskQueue) SetActive(folder, filename string, cancelFunc context.CancelFunc) {
	q.mu.Lock()
	defer q.mu.Unlock()

	key := fmt.Sprintf("%s:%s", folder, filename)
	q.active[key] = cancelFunc
}

//IsActive checks if a task is active
func (q *TaskQueue) IsActive(folder, filename string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	key := fmt.Sprintf("%s:%s", folder, filename)
	_, active := q.active[key]
	return active
}

//TokenBucket for resource tracking
type TokenBucket struct {
	capacity int
	tokens   int
	mu       sync.Mutex
}

func NewTokenBucket(capacity int) *TokenBucket {
	return &TokenBucket{capacity: capacity, tokens: capacity}
}

func (tb *TokenBucket) Acquire() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	if tb.tokens > 0 {
		tb.tokens--
		return true
	}

	return false
}

func (tb *TokenBucket) Release() {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.tokens < tb.capacity {
		tb.tokens++
	}
}

//Woker processes tasks from the queue
func Worker(id int, taskQueue *TaskQueue, tokenBucket *TokenBucket, stopChan chan struct{}) {
	for {
		select {
		case <-stopChan:
			logMessage(fmt.Sprintf("Worker %d stopping...\n", id))
			return
		default:
			cmd, ok := taskQueue.GetCommand()

			if !ok {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			key := fmt.Sprintf("%s:%s", cmd.Folder, cmd.Filename)

			if cmd.Action == "start" {
				if taskQueue.IsActive(cmd.Folder, cmd.Filename) {
					logMessage(fmt.Sprintf("Worker %d: task %s %s is already running. Skipping...\n", id, cmd.Folder, cmd.Filename))
					continue
				}

				if !tokenBucket.Acquire() {
					//logMessage(fmt.Sprintf("Worker %d: insufficent resources for task %s %s. Retrying...\n", id, cmd.Folder, cmd.Filename))
					taskQueue.AddCommand(cmd) //Requeue the task
					time.Sleep(100 * time.Millisecond)
					continue
				}

				ctx, cancel := context.WithCancel(context.Background())
				taskQueue.SetActive(cmd.Folder, cmd.Filename, cancel)

				go func(folder, filename string) {
					defer func() {
						taskQueue.mu.Lock()
						delete(taskQueue.active, key)
						taskQueue.mu.Unlock()
						tokenBucket.Release()
					}()
					runScript(folder, filename, ctx)
				}(cmd.Folder, cmd.Filename)
			}
		}
	}
}

//RunScript executes the command
func runScript(folder, filename string, ctx context.Context) {
	logMessage(fmt.Sprintf("Running script for %s %s\n", folder, filename))
        command := exec.CommandContext(ctx, "python3.12", "hello_world.py", folder, filename)
        output, err := command.CombinedOutput()

        if err != nil {
                logMessage(fmt.Sprintf("Error executing script for %s %s: %v\n", folder, filename, err))
		return
        }

	logMessage(fmt.Sprintf("Script output for %s %s: %s\n", folder, filename, string(output)))
}

//WatchFolder watches a folder for new command files
func WatchFolder(folder string, taskQueue *TaskQueue, stopChan chan struct{}) {
	for {
		select {
		case <-stopChan:
			logMessage(fmt.Sprintln("Stopping folder watcher..."))
			return
		default:
			files, err := os.ReadDir(folder)

			if err != nil {
				logMessage(fmt.Sprintf("Error reading folder: %v\n", err))
				time.Sleep(1 * time.Second)
				continue
			}

			for _, file := range files {
				if !file.IsDir() && strings.HasSuffix(file.Name(), ".txt") {
					commandFile := filepath.Join(folder, file.Name())
					processCommandFile(commandFile, taskQueue)
					os.Remove(commandFile) //Remove the file after processing
				}
			}
			time.Sleep(1 * time.Second)
		}
	}
}

//ProcessCommandFile reads the command from a file and adds it to the queue
func processCommandFile(filePath string, taskQueue *TaskQueue) {
	file, err := os.Open(filePath)
	if err != nil {
		logMessage(fmt.Sprintf("Error opening file: %v\n", err))
		return
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) < 1 {
			logMessage(fmt.Sprintf("Invalid command format in file: %s\n", filePath))
			continue
		}

		if parts[0] == "Shutdown" {
			logMessage(fmt.Sprintln("Shutdown command received."))
			os.Exit(0)
		} else if len(parts) == 3 {
			action, folder, filename := parts[0], parts[1], parts[2]
			if action == "start" || action == "stop" {
				taskQueue.AddCommand(Command{Folder: folder, Filename: filename, Action: action})
			} else {
				logMessage(fmt.Sprintf("Unknown action: %s\n", action))
			}
		} else {
			logMessage(fmt.Sprintf("Invalid command format in file: %s\n", filePath))
		}
	}

	if err := scanner.Err(); err != nil {
		logMessage(fmt.Sprintf("Error reading file: %v\n", err))
	}
}

var (
	logFile     *os.File
	logFileLock sync.Mutex
)

//LogMessage writes the process of the script in text files
func logMessage(message string) {
	logFileLock.Lock()
	defer logFileLock.Unlock()

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	formattedMessage := fmt.Sprintf("%s - %s\n", timestamp, message)

	//Print to console
	fmt.Print(formattedMessage)

	//Write to log file
	if logFile != nil {
		logFile.WriteString(formattedMessage)
	}
}

//SetupLogFile create a new file every top of the hour
func setupLogFile() {
	logFileLock.Lock()
	defer logFileLock.Unlock()
	
	logsDir := "./__LOGS"
	if _, err := os.Stat(logsDir); os.IsNotExist(err) {
		if err := os.Mkdir(logsDir, os.ModePerm); err != nil {
			fmt.Printf("Failed to create logs directory: %v\n", err)
			os.Exit(1)
		}
	}

	//Close existing log file
	if logFile != nil {
		logFile.Close()
	}

	//Create a new log file with the current hour
	now := time.Now()
	logFileName := fmt.Sprintf("process_log_%s.txt", now.Format("2006-01-02_15"))
	logFilePath := filepath.Join(logsDir, logFileName)

	var err error
	logFile, err = os.Create(logFilePath)

	if err != nil {
		fmt.Printf("Failed to create log file: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Start new log file")
}

//Main Function
func main() {
	setupLogFile()

	defer func() {
		logFileLock.Lock()
		if logFile != nil {
			logFile.Close()
		}
		logFileLock.Unlock()
	}()

	ticker := time.NewTicker(1 * time.Hour)
	go func() {
		for range ticker.C {
			setupLogFile()
		}
	}()

	folderToWatch := "./commands"
	numWorkers := runtime.NumCPU()
	tokenBucket := NewTokenBucket(numWorkers*2) //Limit concurrency to 2 tasks

	//Ensure the folder exists
	if _, err := os.Stat(folderToWatch); os.IsNotExist(err) {
		fmt.Printf("Creating folder: %s\n", folderToWatch)
		os.Mkdir(folderToWatch, os.ModePerm)
	}

	taskQueue := NewTaskQueue()
	stopChan := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		WatchFolder(folderToWatch, taskQueue, stopChan)
	}()

	//Start workers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			Worker(workerID, taskQueue, tokenBucket, stopChan)
		}(i)
	}

	//Wait for user interrupt to stop
	fmt.Println("Press Ctrl+C to stop...")
	interruptChan := make(chan os.Signal, 1)

	go func() {
		signal.Notify(interruptChan, os.Interrupt)
		<-interruptChan
		close(stopChan)
		wg.Wait()
		fmt.Println("All workers stopped. Exiting.")
		os.Exit(0)
	}()

	select{}
}
