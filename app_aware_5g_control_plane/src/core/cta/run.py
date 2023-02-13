# This script is used the pktgen to run the experiments.

import sys
import subprocess
import os
from time import sleep
from string import Template

action_string = '''
    {
        cpf = $cpf,
        action = $action,
        config = $config
    }'''

template = """\
serializer=$serializer;
number_of_cpf=$cpfs;
replicas=$replicas;
traffic_distribution=$traffic_distribution;
dequeue_ratios=$dequeue_ratios;
cpfs_action=($cpfs_action_string
);\
"""

def run_async(cmd):
    return subprocess.Popen([cmd], shell=True, stdout=None, stderr=None, close_fds=True, preexec_fn=os.setsid)


def run():
    serializer = sys.argv[1]
    cpfs = sys.argv[2]
    replicas = sys.argv[3]
    cpfs_action = sys.argv[4]
    traffic_distribution = sys.argv[5]
    dequeue_ratios = sys.argv[6]
    

    str_split = lambda x: x.split(',')


    
    config = Template(template)
    cpf_action_template = Template(action_string)

    cpfs_action_decoded = list(map(str_split, cpfs_action.split('_')))
    traffic_distribution_split =  list(map(int, traffic_distribution.split('_')))
    dequeue_ratios_split =  list(map(int, dequeue_ratios.split('_')))

    cpfs_actions = ""
    for i in range(len(cpfs_action_decoded)):
        res = cpf_action_template.substitute({'cpf': cpfs_action_decoded[i][0], 
                                              'action': cpfs_action_decoded[i][1], 
                                              'config': list(map(int, cpfs_action_decoded[i][2:4]))
                                             })
        cpfs_actions += res 
        if i != len(cpfs_action_decoded) - 1:
            cpfs_actions += ","

    # print(cpfs_actions)
    res = config.substitute({'serializer': serializer,
                             'cpfs': cpfs, 'replicas': replicas,
                             'cpfs_action_string': cpfs_actions,
                             'traffic_distribution': traffic_distribution_split,
                             'dequeue_ratios': dequeue_ratios_split})

    config_file = open("cta_config.cfg", "w")
    config_file.write(res)
    config_file.close()

    proc = run_async(
        "sudo ./build/cta -l 0-13 -n 16 --log-level=1 -- -q 8 -p 0,1,2,3 > logs/CTA_console_logs.txt 2>&1")
    proc.wait()

    if os.path.exists("core.lck"):
        os.remove("core.lck")


if __name__ == "__main__":
    run()
