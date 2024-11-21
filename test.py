from dask.distributed import Client

#client = Client(n_workers=4)

from dask_gateway import Gateway
gateway = Gateway(
    "http://dask-gateway-k8s.geddes.rcac.purdue.edu/",
    proxy_address="traefik-dask-gateway-k8s.cms.geddes.rcac.purdue.edu:8786",
)
clusters = gateway.list_clusters()
print (clusters)
cluster_name = clusters[-1].name
client = gateway.connect(cluster_name).get_client()

env = clusters[-1].options["env"]
env.update({"X509_USER_PROXY" : "/work/users/shjeon-cern/x509up_u2005749"})
clusters[-1].options["env"] = env

client

######################################################################

import uproot
try: 
    test = uproot.dask("root://cms-xrd-global.cern.ch//store/mc/Run3Summer23NanoAODv12/QCD-4Jets_HT-400to600_TuneCP5_13p6TeV_madgraphMLM-pythia8/NANOAODSIM/130X_mcRun3_2023_realistic_v14-v3/60000/c5b8293b-65fd-4a14-a25b-2073f9bf6ac7.root:Events")
    print (test["run"].compute())
except:
    print ("test1 failed")
try: 
    test = uproot.dask("root://cms-xcache.rcac.purdue.edu:1094//store/mc/Run3Summer23NanoAODv12/QCD-4Jets_HT-400to600_TuneCP5_13p6TeV_madgraphMLM-pythia8/NANOAODSIM/130X_mcRun3_2023_realistic_v14-v3/60000/c5b8293b-65fd-4a14-a25b-2073f9bf6ac7.root:Events")
    print (test["run"].compute())
except:
    print ("test2 failed")
try: 
    test = uproot.open("root://cms-xrd-global.cern.ch//store/mc/Run3Summer23NanoAODv12/QCD-4Jets_HT-400to600_TuneCP5_13p6TeV_madgraphMLM-pythia8/NANOAODSIM/130X_mcRun3_2023_realistic_v14-v3/60000/c5b8293b-65fd-4a14-a25b-2073f9bf6ac7.root")["Events"]
    print (test["run"].array())
except:
    print ("test3 failed")
try: 
    test = uproot.open("root://cms-xcache.rcac.purdue.edu:1094//store/mc/Run3Summer23NanoAODv12/QCD-4Jets_HT-400to600_TuneCP5_13p6TeV_madgraphMLM-pythia8/NANOAODSIM/130X_mcRun3_2023_realistic_v14-v3/60000/c5b8293b-65fd-4a14-a25b-2073f9bf6ac7.root")["Events"]
    print (test["run"].array())
except:
    print ("test4 failed")

