import sys
import re
import json
import copy

class TransactionManager:
    def __init__(self):
        with open("position.json") as file:
            self.position = json.load(file)
        with open("site_init.json") as file:
            self.site = json.load(file)
        self.transaction = {}


    def begin(self, arg, time):
        self.transaction[arg[0]]={"type":"normal", "status":"normal", "wait_lock":[], "start_time":time}

    def beginro(self, arg, time):
        self.transaction[arg[0]] = {"type": "RO", "status": "normal", "wait_value": [], "start_time": time,
                                    "snapshot":copy.deepcopy(self.site)}

    def write(self, arg, time, wait_site_check):
        transaction = arg[0]
        variable = arg[1]
        value = arg[2]
        if self.transaction[transaction]["status"] == "normal":
            fail_site = []
            already_write_lock = []
            need_obtain_lock = []
            wait_lock = set()
            for site in self.position[variable]:
                if self.site[site]["status"] == 0:
                    fail_site.append(site)
                else:
                    if transaction in self.site[site]["data"][variable]["write_lock"]:
                        already_write_lock.append(site)
                    else:
                        need_obtain_lock.append(site)
                        for trans in self.site[site]["data"][variable]["read_lock"]:
                            wait_lock.add(trans)
                        for trans in self.site[site]["data"][variable]["write_lock"]:
                            wait_lock.add(trans)
            if len(self.position[variable])==len(fail_site):
                self.transaction[transaction]["status"] = "wait_site"
                self.transaction[transaction]["wait_site"] = fail_site
                self.transaction[transaction]["wait_command"] = ["write", variable, value]
                if not wait_site_check:
                    print(transaction + " waits available sites on " + variable)
                return
            wait_lock = list(wait_lock)
            if len(wait_lock) == 1 and transaction in wait_lock:
                flag = 1
                for trans in self.transaction:
                    if transaction!=trans and self.transaction[trans]["status"]=="wait_lock" and self.transaction[trans]["wait_lock"]==[transaction]:
                        flag=0
                        break
                if flag:
                    wait_lock.remove(transaction)
            if not wait_lock:
                for site in already_write_lock:
                    self.site[site]["log"][transaction][variable] = value
                    print(transaction + " write " + variable + "." + site + ": " + str(value))
                for site in need_obtain_lock:
                    if transaction in self.site[site]["data"][variable]["read_lock"]:
                        self.site[site]["data"][variable]["read_lock"].remove(transaction)
                    self.site[site]["data"][variable]["write_lock"] = [transaction]
                    if transaction not in self.site[site]["log"]:
                        self.site[site]["log"][transaction] = {}
                    self.site[site]["log"][transaction][variable] = value
                    print(transaction + " write " + variable + "." + site + ": " + str(value))
            else:
                self.transaction[transaction]["status"] = "wait_lock"
                self.transaction[transaction]["wait_lock"] = wait_lock
                self.transaction[transaction]["wait_lock_time"] = time
                self.transaction[transaction]["wait_command"] = ["write", variable, value]
                print(transaction + " waits write lock on " + variable)

    def read(self, arg, time, wait_site_check):
        transaction = arg[0]
        variable = arg[1]
        if self.transaction[transaction]["type"]=="normal":
            if self.transaction[transaction]["status"]=="normal":
                for site in self.position[variable]:
                    if self.site[site]["status"]==1:
                        if transaction in self.site[site]["data"][variable]["write_lock"]:
                            value = self.site[site]["log"][transaction][variable]
                            print(transaction + " read " + variable + ": " + str(value))
                            return
                        if not self.site[site]["data"][variable]["write_lock"]:
                            if self.site[site]["data"][variable]["read_lock"] and transaction not in self.site[site]["data"][variable]["read_lock"]:
                                for trans in self.transaction:
                                    if self.transaction[trans]["status"]=="wait_lock":
                                        for w in self.site[site]["data"][variable]["read_lock"]:
                                            if w in self.transaction[trans]["wait_lock"]:
                                                self.transaction[transaction]["status"] = "wait_lock"
                                                self.transaction[transaction]["wait_lock"] = self.site[site]["data"][variable]["read_lock"]
                                                self.transaction[transaction]["wait_lock_time"] = time
                                                self.transaction[transaction]["wait_command"] = ["read", variable]
                                                print(transaction+" waits read lock on "+variable)
                                                return
                            if self.site[site]["data"][variable]["can_read"]==1:
                                if transaction not in self.site[site]["data"][variable]["read_lock"]:
                                    self.site[site]["data"][variable]["read_lock"].append(transaction)
                                value = self.site[site]["data"][variable]["value"]
                                print(transaction+" read "+variable+": "+str(value))
                                if transaction not in self.site[site]["log"]:
                                    self.site[site]["log"][transaction] = {}
                                return
                        else:
                            self.transaction[transaction]["status"] = "wait_lock"
                            self.transaction[transaction]["wait_lock"] = self.site[site]["data"][variable]["write_lock"]
                            self.transaction[transaction]["wait_lock_time"] = time
                            self.transaction[transaction]["wait_command"] = ["read", variable]
                            print(transaction+" waits read lock on "+variable)
                            return
                self.transaction[transaction]["status"] = "wait_site"
                self.transaction[transaction]["wait_site"] = self.position[variable]
                self.transaction[transaction]["wait_command"] = ["read", variable]
                if not wait_site_check:
                    print(transaction + " waits available sites on " + variable)

        else:    # RO read
            if self.transaction[transaction]["status"] == "normal":
                if len(self.position[variable])==1:
                    site = self.position[variable][0]
                    if self.site[site]["status"]==1:
                        value = self.transaction[transaction]["snapshot"][site]["data"][variable]["value"]
                        print(transaction + " read " + variable + ": " +str(value))
                    else:
                        self.transaction[transaction]["status"] = "wait_site"
                        self.transaction[transaction]["wait_site"] = self.position[variable]
                        self.transaction[transaction]["wait_command"] = ["read", variable]
                        print(transaction + " waits available sites on " + variable)
                else:
                    waitlist = []
                    for site in self.position[variable]:
                        if self.site[site]["status"] == 1:
                            if not self.site[site]["log"]["fail"] or (self.site[site]["log"]["recover"] and self.site[site]["log"]["recover"]<self.transaction[transaction]["start_time"]):
                                print(transaction+" read "+variable+": "+str(self.transaction[transaction]["snapshot"][site]["data"][variable]["value"]))
                                return
                        else:
                            if self.site[site]["log"]["fail"]>self.transaction[transaction]["start_time"] and (self.transaction[transaction]["snap_shot"][site]["log"]["fail"] or (self.transaction[transaction]["snap_shot"][site]["log"]["recover"] and self.transaction[transaction]["snap_shot"][site]["log"]["recover"]<self.transaction[transaction]["start_time"])):
                                waitlist.append(site)

                    if waitlist:
                        self.transaction[transaction]["status"] = "wait_site"
                        self.transaction[transaction]["wait_site"] = waitlist[:]
                        self.transaction[transaction]["wait_command"] = ["read", variable]
                        print(transaction + " waits available sites on " + variable)
                    else:
                        self.transaction[transaction]["status"] = "abort"
            elif self.transaction[transaction]["status"] == "wait_site":
                flag = 0
                for site in self.transaction[transaction]["wait_site"]:
                    if self.site[site]["status"] == 1:
                        flag = 1
                        print(transaction + " read " + variable + ": " +str(self.transaction[transaction]["snapshot"][site]["data"][variable]["value"]))
                        break
                if flag:
                    self.transaction[transaction]["status"] = "normal"

    def commit(self, arg, time):
        transaction = arg[0]
        if self.transaction[transaction]["status"]=="abort":
            print(transaction, "aborts.")
        elif self.transaction[transaction]["status"]=="normal":
            print(transaction, "commits.")
            for site in self.site:
                if self.site[site]["status"]==1:
                    if transaction in self.site[site]["log"]:
                        for variable in self.site[site]["log"][transaction]:
                            self.site[site]["data"][variable]["value"] = self.site[site]["log"][transaction][variable]
                            self.site[site]["data"][variable]["can_read"] = 1
                            self.site[site]["data"][variable]["time"] = time
        elif self.transaction[transaction]["status"]=="wait_site":
            print(transaction, "aborts.")
        else:
            print("commit error")
        self.clean_up(transaction)

    def fail(self, arg, time):
        site = arg[0]
        self.site[site]["status"] = 0
        self.site[site]["log"]["fail"] = time
        self.site[site]["log"]["recover"] = None
        for trans in self.site[site]["log"]:
            if trans not in ["fail", "recover"]:
                self.transaction[trans]["status"] = "abort"

    def recover(self, arg, time):
        site = arg[0]
        self.site[site]["status"] = 1
        self.site[site]["log"] = {"fail": self.site[site]["log"]["fail"], "recover": time}
        for variable in self.site[site]["data"]:
            self.site[site]["data"][variable]["read_lock"] = []
            self.site[site]["data"][variable]["write_lock"] = []
            if len(self.position[variable])==1:
                self.site[site]["data"][variable]["can_read"] = 1
            else:
                self.site[site]["data"][variable]["can_read"] = 0

    def dump(self):
        for site in self.site:
            print("site " + site + "- ", end="")
            for variable in self.site[site]["data"]:
                print(variable+": "+str(self.site[site]["data"][variable]["value"]), end=", ")
            print("")

    def detect_cycle(self):
        reverse_dict = {}
        for trans in self.transaction:
            if self.transaction[trans]["status"]=="wait_lock":
                for lock in self.transaction[trans]["wait_lock"]:
                    if lock in reverse_dict:
                        reverse_dict[lock].append([trans, self.transaction[trans]["wait_lock_time"]])
                    else:
                        reverse_dict[lock] = [[trans, self.transaction[trans]["wait_lock_time"]]]
                self.transaction[trans]["wait_lock"] = []

        for key in reverse_dict:
            reverse_dict[key].sort(key=lambda a:a[1])
            self.transaction[reverse_dict[key][0][0]]["wait_lock"] = [key]
            for i in range(1, len(reverse_dict[key])):
                self.transaction[reverse_dict[key][i][0]]["wait_lock"].append(reverse_dict[key][i-1][0])
        # cycle detect
        done = []
        seen = []
        def cycle(trans):
            for tran in trans:
                if tran in done:
                    continue
                if tran in seen:
                    return False
                seen.append(tran)
                if self.transaction[tran]["status"] == "wait_lock":
                    if not cycle(self.transaction[tran]["wait_lock"]):
                        return False
                done.append(tran)
            return True
        flag = 1
        for trans in self.transaction:
            if self.transaction[trans]["status"] == "wait_lock":
                if trans in done:
                    continue
                if not cycle([trans]):
                    flag=0
                    break
        if flag:
            return
        else:
            largest = 0
            yongest_trans = ""
            for trans in self.transaction:
                if self.transaction[trans]["status"] == "wait_lock":
                    if self.transaction[trans]["start_time"]>largest:
                        largest = self.transaction[trans]["start_time"]
                        yongest_trans = trans
            print("Cycle detected. Abort youngest transaction: "+yongest_trans)
            self.clean_up(yongest_trans)
        self.detect_cycle()
        self.check_wait_lock()

    def clean_up(self, trans):
        self.transaction.pop(trans)
        for site in self.site:
            if self.site[site]["status"] == 1:
                if trans in self.site[site]["log"]:
                    self.site[site]["log"].pop(trans)
            for data in self.site[site]["data"]:
                if trans in self.site[site]["data"][data]["read_lock"]:
                    self.site[site]["data"][data]["read_lock"].remove(trans)
                if trans in self.site[site]["data"][data]["write_lock"]:
                    self.site[site]["data"][data]["write_lock"].remove(trans)
        for tran in self.transaction:
            if self.transaction[tran]["status"] == "wait_lock" and trans in self.transaction[tran]["wait_lock"]:
                self.transaction[tran]["wait_lock"].remove(trans)
        self.detect_cycle()
        self.check_wait_lock()

    def check_wait_site(self):
        for transaction in self.transaction:
            if self.transaction[transaction]["status"] == "wait_site":
                if self.transaction[transaction]["type"] == "RO":
                    self.read([transaction, self.transaction[transaction]["wait_command"][1]], self.transaction[transaction]["start_time"], True)
                elif self.transaction[transaction]["wait_command"][0] == "read":
                    self.transaction[transaction]["status"] = "normal"
                    self.read([transaction, self.transaction[transaction]["wait_command"][1]], self.transaction[transaction]["start_time"], True)
                else:
                    self.transaction[transaction]["status"] = "normal"
                    self.read([transaction, self.transaction[transaction]["wait_command"][1],self.transaction[transaction]["wait_command"][2]],self.transaction[transaction]["start_time"], True)




    def check_wait_lock(self):
        for transaction in self.transaction:
            if self.transaction[transaction]["status"] == "wait_lock" and not self.transaction[transaction]["wait_lock"]:
                self.transaction[transaction]["status"] = "normal"
                command = self.transaction[transaction]["wait_command"]
                if command[0]=="read":
                    self.read([transaction, command[1]], self.transaction[transaction]["wait_lock_time"], False)
                    for trans in self.transaction:
                        if self.transaction[trans]["status"] == "wait_lock" and transaction in self.transaction[trans]["wait_lock"] and self.transaction[trans]["wait_command"][0]=="read":
                            self.transaction[trans]["wait_lock"].remove(transaction)
                            self.check_wait_lock()         
                elif command[0]=="write":
                    self.write([transaction, command[1], command[2]], self.transaction[transaction]["wait_lock_time"], False)


    def read_command(self, command, time):
        com = re.split("\(|\)", command)
        if len(com) != 3:
            print("Syntex error on:", command)
            return
        if com[0] == "begin":
            args = com[1].split(",")
            if len(args) != 1:
                print("Syntex error on:", command)
                return
            self.begin(args, time)
        elif com[0] == "beginRO":
            args = com[1].split(",")
            if len(args) != 1:
                print("Syntex error on:", command)
                return
            self.beginro(args, time)
        elif com[0] == "W":
            args = com[1].split(",")
            if len(args) != 3:
                print("Syntex error on:", command)
                return
            self.write(args, time, False)
            self.detect_cycle()
        elif com[0] == "R":
            args = com[1].split(",")
            if len(args) != 2:
                print("Syntex error on:", command)
                return
            self.read(args, time, False)
            self.detect_cycle()
        elif com[0] == "end":
            args = com[1].split(",")
            if len(args) != 1:
                print("Syntex error on:", command)
                return
            self.commit(args, time)
            self.check_wait_site()
        elif com[0] == "fail":
            args = com[1].split(",")
            if len(args) != 1:
                print("Syntex error on:", command)
                return
            self.fail(args, time)
        elif com[0] == "recover":
            args = com[1].split(",")
            if len(args) != 1:
                print("Syntex error on:", command)
                return
            self.recover(args, time)
            self.check_wait_site()
        elif com[0] == "dump":
            args = com[1].split(",")
            if len(args) != 1:
                print("Syntex error on:", command)
                return
            self.dump()
        else:
            print("Syntex error on:", command)

if __name__ == '__main__':
    tm = TransactionManager()
    if len(sys.argv)==1:
        print("Read from standard input. Type the instructions here.\nType exit to end program.\n--------------")
        time=1
        while True:
            command = input().split("//")[0].strip().replace(" ","")
            if command=="exit" :
                print("Program end.")
                exit(0)
            if len(command)<=3:
                continue
            tm.read_command(command.strip(), time)
            time+=1
    elif len(sys.argv)==2:
        print("Read instructions from file:", sys.argv[1],"\n--------------")
        file = open(sys.argv[1], 'r')
        lines = file.readlines()
        time=1
        for line in lines:
            line = line.split("//")[0].strip().replace(" ","")
            if len(line)<=3:
                continue
            tm.read_command(line, time)
            time+=1
    else:
        print("Command error.")

