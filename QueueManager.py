import os
from pathlib import Path 
import shutil

def read_json(path):
    # helper function to read json contents from filepath
    with open(path, "r") as fp:
        return json.loads(fp.read())

def write_json(path, data):
    # helper function to write json data to filepath
    with open(path, "w") as fp:
        fp.write(json.dumps(data))

class QueueManager:

    def __init__(self, base):
        # base directory, where all other queues live
        self.base = Path(base).absolute()
        # path to failed jobs directory
        self.failed = self.base / "failed"
        # path to nested directory of inprocess jobs for each device
        self.inprocess = self.base / "inprocess"
        # path to tmp dir which holds configs and output/error logs for jobs
        self.tmp = self.base / "tmp"
        # path to nested dir for each queue containing waiting jobs
        self.waiting = self.base / "waiting"

    def queue_range(self, src):
        # get the minimum and maximum numbers from a queue
        items = [int(x) for x in os.listdir(src)]
        if len(items) == 0:
            return (-1,-1)
        return (min(items), max(items))

    def swap_waiting_queue_items(self, a, a_num, b, b_num):
        # swap queue items
        # a: name of first queue in waiting, ex "main", "example_pipeline", etc
        # a_num: number of item to swap from a
        # b: name of second queue in waiting, can be the same or different as a
        # b_num: number of item to swap from b
        a_path = self.waiting / Path(a) / str(a_num)
        a_data = read_json(a_path)
        b_path = self.waiting / Path(b) / str(b_num)
        b_data = read_json(b_path)
        write_json(b_path, a_data)
        write_json(a_path, b_data)

    def move_waiting_queue_item(self, src, src_num, dst, dst_num):
        # move queue item from waiting/src/src_num to waiting/dst/dst_num
        src_path = self.waiting / Path(src) / str(src_num)
        dst_path = self.waiting / Path(dst) / str(dst_num)
        print(src_path, dst_path)
        shutil.move(src_path, dst_path)

    def send_waiting_to_front(self, src, src_num, dst=None):
        # move a waiting job from waiting/src/src_num to waiting/dst/{front}
        if dst is None:
            dst = src
        dst_min, dst_max = self.queue_range(self.waiting / dst)
        if dst_min == -1 and dst_max == -1:
            dst_min = 1
        if dst_min == 0 and dst_min != 0:
            raise ValueError("Cannot move to front now -- please wait until first job finishes")
        if dst_min == 0 and dst_max == 0:
            return self.send_waiting_to_back(src, src_num, dst)
        if src == dst and src_num == dst_num:
            return
        dst_num = dst_min - 1
        self.move_waiting_queue_item(src, src_num, dst, dst_num)

    def send_waiting_to_back(self, src, src_num, dst=None):
        # move a waiting job from waiting/src/src_num to waiting/dst/{back}
        if dst is None:
            dst = src
        _, dst_max = self.queue_range(self.waiting / dst)
        dst_num = dst_max + 1
        self.move_waiting_queue_item(src, src_num, dst, dst_num)
        

if __name__ == "__main__":
    qm = QueueManager("./example_queue_structure")
    print(qm.base)
    qm.send_waiting_to_back("main", 21)