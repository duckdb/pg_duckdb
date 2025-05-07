from contextlib import contextmanager
from multiprocessing import Process, Queue

from .motherduck_token_helper import create_test_user
from .utils import create_duckdb


class MDClientResponder:
    def __init__(self, res_queue):
        self.res_queue = res_queue

    def ok(self, result=None):
        self.res_queue.put({"success": True, "result": result})

    def ko(self, error):
        self.res_queue.put({"success": False, "error": error})


class MDClient:
    def __init__(self, db_name):
        self.cmd_queue = Queue()
        self.res_queue = Queue()
        self.process = Process(
            target=MDClient.run,
            args=(
                self.cmd_queue,
                self.res_queue,
            ),
        )
        self.process.start()
        self.send("createddb", [db_name])

    @staticmethod
    @contextmanager
    def create(*args):
        clients = ()
        for arg in args:
            clients += (MDClient(arg),)

        try:
            yield clients
        finally:
            for client in clients:
                client.terminate()

    @staticmethod
    def run(cmd_queue, res_queue):
        responder = MDClientResponder(res_queue)
        token = create_test_user()["token"]
        ddb_con = None
        while True:
            data = cmd_queue.get()
            cmd = data["command"]
            try:
                if cmd == "terminate":
                    responder.ok()
                    break
                elif cmd == "createddb":
                    db_name = data["args"][0]
                    ddb_con = create_duckdb(db_name, token)
                    responder.ok()
                elif cmd == "run_query":
                    r = ddb_con.execute(data["args"][0])
                    responder.ok(r.fetchall())
                elif cmd == "get_token":
                    responder.ok(token)
                else:
                    responder.ok(False)
            except Exception as e:
                responder.ko(e)

    def send(self, command, args=None):
        self.cmd_queue.put({"command": command, "args": args})
        res = self.res_queue.get()
        if res["success"]:
            return res["result"]
        else:
            raise res["error"]

    def get_token(self):
        return self.send("get_token")

    def run_query(self, query):
        return self.send("run_query", [query])

    def terminate(self):
        self.send("terminate")
        self.process.join()
