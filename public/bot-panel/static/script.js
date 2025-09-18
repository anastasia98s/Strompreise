new Vue({
    el: '#app',
    data: {
        appStyle: {display: 'flex'},
        schema: null,
        ws: null,
        outputLog: '',
        outputTimer: null,
        outputNumWorkers: null,
        outputProcess: null,
        outputSchedulerInterval: null,
        outputPauseTimer: null,
        outputVerboseLog: null,
        outputSaveJsonDb: null,
        outputSaveJsonFile: null,
        outputNumTasks: [],
        isScrolledToBottom: true,
        inputResetSession: false
    },
    computed: {
        formattedTime() {
            let seconds = this.outputTimer
            let hrs   = Math.floor(seconds / 3600)
            let mins  = Math.floor((seconds % 3600) / 60)
            let secs  = seconds % 60

            return [hrs, mins, secs]
                .map(v => v.toString().padStart(2, "0"))
                .join(":")
        }
    },
    methods: {
        logOutput(message) {
            this.outputLog += message + "\n";
            this.$nextTick(() => {
                if (this.isScrolledToBottom) {
                    this.scrollToBottom();
                }
            });
        },
        scrollToBottom() {
            const terminal = this.$refs.terminal;
            if (terminal) {
                terminal.scrollTop = terminal.scrollHeight;
            }
        },
        handleScroll() {
            const terminal = this.$refs.terminal;
            if (terminal) {
                const threshold = 50; // pixels from bottom
                this.isScrolledToBottom = terminal.scrollTop + terminal.clientHeight >= terminal.scrollHeight - threshold;
            }
        },
        addWorker() {
            if (this.ws) {
                this.ws.send(JSON.stringify({
                    action: "add_worker",
                    data: null
                }));
            }
        },
        removeWorker() {
            if (this.ws) {
                this.ws.send(JSON.stringify({
                    action: "remove_worker",
                    data: null
                }));
            }
        },
        setProcess() {
            if (this.ws) {
                this.ws.send(JSON.stringify({
                    action: "set_process",
                    data: this.inputResetSession
                }));
            }
        },
        setSchedulerInterval() {
            if (typeof this.outputSchedulerInterval !== "number" || isNaN(this.outputSchedulerInterval) || this.outputSchedulerInterval < 1) {
                this.logOutput("Please enter a valid number greater than or equal to 1!");
                return;
            }
            if (this.ws) {
                this.ws.send(JSON.stringify({
                    action: "set_scheduler_interval",
                    data: this.outputSchedulerInterval-1
                }));
            }
        },
        setPauseTimer() {
            if (this.ws) {
                this.ws.send(JSON.stringify({
                    action: "set_pause_timer",
                    data: null
                }));
            }
        },
        setVerboseLog() {
            if (this.ws) {
                this.ws.send(JSON.stringify({
                    action: "set_verbose_log",
                    data: null
                }));
            }
        },
        setTasks() {
            if (this.ws) {
                this.ws.send(JSON.stringify({
                    action: "set_tasks",
                    data: null
                }));
            }
        },
        setSaveJsonDb() {
            if (this.ws) {
                this.ws.send(JSON.stringify({
                    action: "set_save_json_db",
                    data: null
                }));
            }
        },
        setSaveJsonFile() {
            if (this.ws) {
                this.ws.send(JSON.stringify({
                    action: "set_save_json_file",
                    data: null
                }));
            }
        },
        impotGeos() {
            if (this.ws) {
                this.ws.send(JSON.stringify({
                    action: "import_geos_from_csv",
                    data: null
                }));
            }
        },
        handleLog(data) {
            const timestamp = new Date().toLocaleTimeString();
            this.logOutput(`[${timestamp}] ${data}`);
        },
        handleTimer(data) {
            this.outputTimer = data;
        },
        handleNumWorkers(data) {
            this.outputNumWorkers = data;
        },
        handleProcess(data) {
            this.outputProcess = data;
        },
        handleSchedulerInterval(data) {
            this.outputSchedulerInterval = data+1;
        },
        handlePauseTimer(data) {
            this.outputPauseTimer = data;
        },
        handleVerboseLog(data) {
            this.outputVerboseLog = data;
        },
        handleSaveJsonDb(data) {
            this.outputSaveJsonDb = data;
        },
        handleSaveJsonFile(data) {
            this.outputSaveJsonFile = data;
        },
        handleNumTask(data) {
            if (data === undefined || data === null) return;

            function setNumTask(arr, newItem) {
                const index = arr.findIndex(item => item.id === newItem.id);
                if (index !== -1) {
                    arr[index] = newItem;
                } else {
                    arr.push(newItem);
                }
            }

            setNumTask(this.outputNumTasks, data);
        }
    },
    mounted() {
        //this.ws = new WebSocket("ws://localhost:8000/api/bot_panel/bot_panel_ws");

        const wsProtocol = window.location.protocol === "https:" ? "wss" : "ws";
        const wsHost = window.location.host;
        this.ws = new WebSocket(`${wsProtocol}://${wsHost}/api/bot_panel/bot_panel_ws`);

        this.ws.onopen = () => {
            this.logOutput("Connected to Server");
        };

        const actionMap = {
            get_log: this.handleLog,
            get_timer: this.handleTimer,
            get_num_workers: this.handleNumWorkers,
            get_process: this.handleProcess,
            get_scheduler_interval: this.handleSchedulerInterval,
            get_pause_timer: this.handlePauseTimer,
            get_verbose_log: this.handleVerboseLog,
            get_save_json_db: this.handleSaveJsonDb,
            get_save_json_file: this.handleSaveJsonFile,
            get_num_task: this.handleNumTask
        };

        this.ws.onmessage = (event) => {
            if (event.data === undefined || event.data === null) return;
            //console.log(event.data);
            try {
                const json_data = JSON.parse(event.data);
                if (json_data.action && actionMap[json_data.action]) {
                    actionMap[json_data.action].call(this, json_data.data);
                } else {
                    this.logOutput("Unknown action: " + json_data.action);
                }
            } catch (err) {
                this.logOutput(`Error parsing JSON: ${err.message} | Raw data: ${event.data}`);
            }
        };

        this.ws.onclose = () => {
            this.logOutput("Disconnected from Server");
        };

        this.ws.onerror = (err) => {
            this.logOutput("Connection Error: " + err);
        };

        // Initialize scroll position tracking
        this.$nextTick(() => {
            this.scrollToBottom();
        });
    }
});