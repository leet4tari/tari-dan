//  Copyright 2024 The Tari Project
//  SPDX-License-Identifier: BSD-3-Clause

import { ChangeEvent, useEffect, useState } from "react";
import { jsonRpc } from "../utils/json_rpc";
import { ExecutedTransaction } from "../Types.ts";
import MinotariWallet from "../components/MinotariWallet";
import NodeControls from "../components/NodeControls.tsx";
import MinotariNodes from "../components/MinotariNodes.tsx";

enum Executable {
  BaseNode = 1,
  Wallet = 2,
  Miner = 3,
  ValidatorNode = 4,
  Indexer = 5,
  DanWallet = 6,
  Templates = 7,
}

async function jsonRpc2(address: string, method: string, params: any = null) {
  let id = 0;
  id += 1;
  const response = await fetch(address, {
    method: "POST",
    body: JSON.stringify({
      method: method,
      jsonrpc: "2.0",
      id: id,
      params: params,
    }),
    headers: {
      "Content-Type": "application/json",
    },
  });
  const json = await response.json();
  if (json.error) {
    throw json.error;
  }
  return json.result;
}

function ExtraInfoVN({ name, url, addTxToPool, autoRefresh, state, horizontal }: {
  name: string,
  url: string,
  addTxToPool: any,
  autoRefresh: boolean,
  state: any,
  horizontal: boolean
}) {
  const [epochManagerStats, setEpochManagerStats] = useState<any>(null);
  const [consensusStatus, setConsensusStatus] = useState<any>(null);
  const [pool, setPool] = useState([]);
  const [copied, setCopied] = useState<string | null>(null);
  const [missingTxStates, setMissingTxStates] = useState({}); // {tx_id: [vn1, vn2, ...]}
  const [publicKey, setPublicKey] = useState(null);
  const [peerId, setPeerId] = useState(null);
  const [tick, setTick] = useState(0);
  useEffect(() => {
    if (autoRefresh) {
      const timer = setInterval(() => {
        setTick(tick + 1);
      }, 1000);
      return () => clearInterval(timer);
    }
  }, [tick, autoRefresh]);
  useEffect(() => {
    jsonRpc2(url, "get_epoch_manager_stats").then((resp) => {
      setEpochManagerStats(resp);
    }).catch((resp) => {
      console.error("err", resp);
    });
    jsonRpc2(url, "get_consensus_status").then((resp) => {
      setConsensusStatus(resp);
    }).catch((resp) => {
      console.error("err", resp);
    });
    jsonRpc2(url, "get_tx_pool").then((resp) => {
      setPool(resp.tx_pool);
      addTxToPool(resp.tx_pool.filter((tx: any) => Boolean(tx?.transaction)).map((tx: any) => tx.transaction.id).sort());
    }).catch((resp) => {
      console.error("err", resp);
    });
    jsonRpc2(url, "get_identity").then((resp) => {
      setPublicKey(resp.public_key);
      setPeerId(resp.peer_id);
    }).catch((resp) => {
      console.error("err", resp);
    });
    let missing_tx = new Set();
    for (const k in state) {
      if (k != name && state[k].length > 0) {
        missing_tx = new Set([...missing_tx, ...state[k]]);
      }
    }
    const my_txs = new Set(state[name]);
    missing_tx = new Set([...missing_tx].filter((tx) => !my_txs.has(tx)));
    const promises = Array.from(missing_tx).map((tx) => jsonRpc2(url, "get_transaction", [tx])
      .then((resp) => resp.transaction as ExecutedTransaction)
      .catch((resp) => {
        throw { resp, tx };
      }));
    Promise.allSettled(promises).then((results) => {
      const newState: Map<string, any> = new Map();
      for (const result of results) {
        if (result.status == "fulfilled") {
          const tx = result.value;
          newState.set(tx.transaction.id, {
            known: true,
            abort_details: tx.abort_details,
            final_decision: tx.final_decision,
          });
        } else {
          newState.set(result.reason.tx, { known: false });
        }
      }
      if (JSON.stringify(newState) != JSON.stringify(missingTxStates)) {
        setMissingTxStates(newState);
      }
    }).catch((resp) => {
      console.error("all settled err", resp);
    });
    // for (let tx of missing_tx) {
    //   jsonRpc2(url, "get_transaction", [tx]).then((resp) => {
    //     setMissingTxStates((state) => ({ ...state, [tx]: { known: true, abort_details: resp.transaction.abort_details, final_decision: resp.transaction.final_decision } }));
    //     // console.log(resp);
    //   }).catch((resp) => { setMissingTxStates((state) => ({ ...state, [tx]: { know: false } })); });
    // }
  }, [tick, state]);
  const shorten = (str: string) => {
    if (str.length > 20) {
      return str.slice(0, 3) + "..." + str.slice(-3);
    }
    return str;
  };
  useEffect(() => {
    if (copied) {
      setTimeout(() => setCopied(null), 1000);
    }
  }, [copied]);
  const copyToClipboard = (str: string) => {
    setCopied(str);
    navigator.clipboard.writeText(str);
  };
  const showMissingTx = (missingTxStates: { [key: string]: any }) => {
    if (Object.keys(missingTxStates).length == 0) {
      return null;
    }
    return (
      <>
        <hr />
        <h3>Transaction from others TXs pools</h3>
        <div style={{
          display: "grid",
          gridAutoFlow: horizontal ? "column" : "row",
          gridTemplateRows: horizontal ? "auto auto auto auto" : "auto",
          gridTemplateColumns: horizontal ? "auto" : "auto auto auto auto",
        }}>
          <b>Tx Id</b>
          <b>Known</b>
          <b>Abort details</b>
          <b>Final decision</b>
          {Object.keys(missingTxStates).map((tx) => {
            const { known, abort_details, final_decision } = missingTxStates[tx];
            return (
              <>
                <div onClick={() => copyToClipboard(tx)}>{copied == tx ? "Copied" : shorten(tx)}</div>
                <div style={{ color: known ? "green" : "red" }}><b>{known && "Yes" || "No"}</b></div>
                <div>{abort_details || <i>unknown</i>}</div>
                <div>{("Abort" in final_decision) ? <>Abort ({final_decision.Abort})</> : <>Commit</>}</div>
              </>
            );
          })}
        </div>
      </>);
  };
  const showPool = (pool: Array<any>) => {
    if (pool.length == 0) {
      return null;
    }
    return (<>
        <hr />
        <h3>Pool transactions {pool.length}</h3>
        <table style={{
          width: "100%",
        }}>
          <tr>
            <td>Tx Id</td>
            <td>Ready</td>
            <td>Decision</td>
            <td>Stage</td>
          </tr>
          {pool.map((rec, i) => (
            <tr key={i}>
              <td
                onClick={() => copyToClipboard(rec.transaction_id)}>{copied && "Copied" || shorten(rec.transaction_id)}</td>
              <td>{(rec.is_ready) ? "Yes" : "No"}</td>
              <td>{getDecision(rec)}</td>
              <td>{rec.stage}</td>
            </tr>))}
        </table>
      </>
    );
  };

  const {
    committee_info: committeeInfo,
    current_block_height: baseLayerheight,
    current_epoch: baseLayerEpoch,
    start_epoch: startEpoch,
  } = epochManagerStats || {} as any;

  const {
    height: consensusHeight,
    epoch: consensusEpoch,
    state: consensusState,
  } = consensusStatus || {} as any;

  return (
    <div style={{ whiteSpace: "nowrap" }}>
      <hr />
      <div style={{
        display: "grid",
        gridAutoFlow: "column",
        gridTemplateColumns: "auto auto",
        gridTemplateRows: "auto auto auto auto auto",
      }}>
        <div><b>Shard Group</b></div>
        <div><b>Base layer</b></div>
        <div><b>Consensus</b></div>
        <div><b>Public key</b></div>
        <div><b>Peer id</b></div>
        <div>{committeeInfo ? `${committeeInfo?.shard_group.start}-${committeeInfo?.shard_group.end_inclusive} (${committeeInfo?.num_shard_group_members} members)` : "--"}</div>
        <div>Height: {baseLayerheight},
          Epoch: {baseLayerEpoch} {startEpoch ? `  (since epoch ${startEpoch})` : " <inactive>"}</div>
        <div>Height: {consensusHeight}, Epoch: {consensusEpoch}, Status: {consensusState}</div>
        <div>{publicKey}</div>
        <div>{peerId}</div>
      </div>
      {showPool(pool)}
      {showMissingTx(missingTxStates)}
    </div>
  );
}

function ShowInfo(params: any) {
  const {
    children,
    executable,
    name,
    node,
    logs,
    stdoutLogs,
    showLogs,
    autoRefresh,
    updateState,
    state,
    horizontal,
    onReload,
  } = params;

  const nameInfo = name && (
    <div>
      <pre></pre>
      <b>Name</b>
      {name}
    </div>
  );
  const jrpcInfo = node?.jrpc && (
    <div>
      <b>JRPC</b>
      <a href={`${node.jrpc}/json_rpc`} target="_blank">{node.jrpc}/json_rpc</a>
    </div>
  );
  const grpcInfo = node?.grpc && (
    <div>
      <b>GRPC</b>
      <span className="select">{node.grpc}</span>
    </div>
  );
  const httpInfo = node?.web && (
    <div>
      <b>HTTP</b>
      <a href={node.web} target="_blank">{node.web}</a>
    </div>
  );
  const logInfo = logs && (
    <>
      <div>
        <b>Logs</b>
        <div>
          {logs?.map((e: any) => (
            <div key={e[0]}>
              <a href={`log/${btoa(e[0])}/normal`}>
                {e[1]} - {e[2]}
              </a>
            </div>
          ))}
        </div>
      </div>
      <div>
        <div>
          {stdoutLogs?.map((e: any) => (
            <div key={e[0]}>
              <a href={`log/${btoa(e[0])}/stdout`}>stdout</a>
            </div>
          ))}
        </div>
      </div>
    </>
  );
  const addTxToPool = (tx: any) => {
    updateState({ name: name, state: tx });
  };

  const handleOnStart = () => {
    jsonRpc("start_instance", { by_id: node.instance_id }).then(onReload);
  };

  const handleOnStop = () => {
    jsonRpc("stop_instance", { by_id: node.instance_id }).then(onReload);
  };

  const handleDeleteData = () => {
    jsonRpc("delete_data", { by_id: node.instance_id }).then(onReload);
  };


  return (
    <div className="info" key={name}>
      {nameInfo}
      {httpInfo}
      {jrpcInfo}
      {grpcInfo}
      {showLogs && logInfo}
      {executable === Executable.ValidatorNode && node?.jrpc &&
        <ExtraInfoVN name={name} url={node.jrpc} addTxToPool={addTxToPool} autoRefresh={autoRefresh}
                     state={state}
                     horizontal={horizontal} />}
      {executable !== Executable.Templates &&
        <NodeControls
          isRunning={node?.is_running || false}
          onStart={() => handleOnStart()}
          onStop={() => handleOnStop()}
          onDeleteData={() => handleDeleteData()}
        />}
      {children}
    </div>
  );
}


function ShowInfos(params: any) {
  const { nodes, logs, stdoutLogs, name, showLogs, autoRefresh, horizontal, onReload } = params;
  const [state, setState] = useState<{ [key: string]: any }>({});
  let executable: Executable;
  switch (name) {
    case "vn":
      executable = Executable.ValidatorNode;
      break;
    case "dan":
      executable = Executable.DanWallet;
      break;
    case "indexer":
      executable = Executable.Indexer;
      break;
    default:
      console.log(`Unknown name ${name}`);
      break;
  }
  const updateState = (partial_state: { name: string, state: any }) => {
    if (JSON.stringify(state[partial_state.name]) != JSON.stringify(partial_state.state)) {
      setState((state) => ({ ...state, [partial_state.name]: partial_state.state }));
    }
  };

  const sortedNodes = Object.keys(nodes).map((key) => [key, nodes[key]]);
  sortedNodes.sort((a, b) => {
    if (a[1].instance_id > b[1].instance_id) {
      return 1;
    }
    if (a[1].instance_id < b[1].instance_id) {
      return -1;
    }
    return 0;
  });

  return (
    <div className="infos" style={{ display: "grid" }}>
      {sortedNodes.map(([key, node]) =>
        <ShowInfo key={key} executable={executable} name={node.name} node={node}
                  logs={logs?.[`${name} ${key}`]} stdoutLogs={stdoutLogs?.[`${name} ${key}`]}
                  showLogs={showLogs}
                  autoRefresh={autoRefresh} updateState={updateState} state={state} horizontal={horizontal}
                  onReload={onReload} />)}
    </div>
  );
}

export default function Main() {
  const [vns, setVns] = useState({});
  const [danWallet, setDanWallets] = useState({});
  const [indexers, setIndexers] = useState({});
  const [logs, setLogs] = useState<any | null>({});
  const [stdoutLogs, setStdoutLogs] = useState<any | null>({});
  const [connectorSample, setConnectorSample] = useState(null);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [showLogs, setShowLogs] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [horizontal, setHorizontal] = useState(false);
  const [instances, setInstances] = useState<any>([]);
  const [isMining, setIsMining] = useState<boolean>(false);
  const [miningInterval, setMiningInterval] = useState<number>(120);

  const getInfo = () => {
    jsonRpc("vns")
      .then((resp) => {
        setVns(resp.nodes);
        Object.keys(resp.nodes).map((index) => {
          jsonRpc("get_logs", `vn ${index}`)
            .then((resp) => {
              setLogs((state: any) => ({ ...state, [`vn ${index}`]: resp }));
            })
            .catch((error) => console.log(error));
          jsonRpc("get_stdout", `vn ${index}`)
            .then((resp) => {
              setStdoutLogs((state: any) => ({ ...state, [`vn ${index}`]: resp }));
            })
            .catch((error) => console.log(error));
        });
      })
      .catch((error) => {
        console.log(error);
      });
    jsonRpc("dan_wallets")
      .then((resp) => {
        setDanWallets(resp.nodes);
        Object.keys(resp.nodes).map((index) => {
          jsonRpc("get_logs", `dan ${index}`)
            .then((resp) => {
              setLogs((state: any) => ({ ...state, [`dan ${index}`]: resp }));
            })
            .catch((error) => console.log(error));
          jsonRpc("get_stdout", `dan ${index}`)
            .then((resp) => {
              setStdoutLogs((state: any) => ({ ...state, [`dan ${index}`]: resp }));
            })
            .catch((error) => console.log(error));
        });
      })
      .catch((error) => {
        console.log(error);
      });
    jsonRpc("indexers")
      .then((resp) => {
        setIndexers(resp.nodes);
        Object.keys(resp.nodes).map((index) => {
          jsonRpc("get_logs", `indexer ${index}`)
            .then((resp) => {
              setLogs((state: any) => ({ ...state, [`indexer ${index}`]: resp }));
            })
            .catch((error) => console.log(error));
          jsonRpc("get_stdout", `indexer ${index}`)
            .then((resp) => {
              setStdoutLogs((state: any) => ({ ...state, [`indexer ${index}`]: resp }));
            })
            .catch((error) => console.log(error));
        });
      })
      .catch((error) => {
        console.log(error);
      });
    jsonRpc("http_connector")
      .then((resp) => {
        setConnectorSample(resp);
      })
      .catch((error) => {
        console.log(error);
      });
    jsonRpc("get_logs", "node").then((resp) => {
      setLogs((state: any) => ({ ...state, node: resp }));
    });
    jsonRpc("get_logs", "wallet").then((resp) => {
      setLogs((state: any) => ({ ...state, wallet: resp }));
    });
    jsonRpc("get_logs", "miner").then((resp) => {
      setLogs((state: any) => ({ ...state, miner: resp }));
    });
    jsonRpc("get_stdout", "node").then((resp) => {
      setStdoutLogs((state: any) => ({ ...state, node: resp }));
    });
    jsonRpc("get_stdout", "wallet").then((resp) => {
      setStdoutLogs((state: any) => ({ ...state, wallet: resp }));
    });
    jsonRpc("get_stdout", "miner").then((resp) => {
      setStdoutLogs((state: any) => ({ ...state, miner: resp }));
    });
    jsonRpc("list_instances", { by_type: null }).then(({ instances }) => setInstances(instances));
    jsonRpc("is_mining", {}).then((resp: { result: boolean }) => {
      setIsMining(resp.result);
    });
  };

  useEffect(getInfo, []);

  const handleFileChange = (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.item(0);
    if (file) {
      setSelectedFile(file);
    }
  };

  const handleFileUpload = () => {
    if (!selectedFile) {
      return;
    }
    const address = import.meta.env.VITE_DAEMON_JRPC_ADDRESS || ""; //Current host
    const formData = new FormData();
    formData.append("file", selectedFile);
    fetch(`${address}/upload_template`, { method: "POST", body: formData }).then((resp) => {
      console.log("resp", resp);
    });
  };

  const stopAll = () => {
    jsonRpc("stop_all", { instance_type: "TariValidatorNode" }).then(getInfo);
  };

  const startAll = () => {
    jsonRpc("start_all", { instance_type: "TariValidatorNode" }).then(getInfo);
  };

  const addValidatorNode = () => {
    jsonRpc("add_validator_node", { name: null, register: true, mine: false }).then(getInfo);
  };

  return (
    <div className="main">
      <button onClick={() => stopAll()}>Stop all VNs</button>
      <button onClick={() => startAll()}>Start all VNs</button>
      <button onClick={() => setShowLogs(!showLogs)}>{showLogs && "Hide" || "Show"} logs</button>
      <button onClick={() => setAutoRefresh(!autoRefresh)}>{autoRefresh && "Disable" || "Enable"} autorefresh
      </button>
      <button onClick={() => setHorizontal(!horizontal)}>Swap rows/columns</button>
      <button onClick={() => addValidatorNode()}>Add VN</button>
      <div className="label">Base layer</div>
      <div className="infos">
        <MinotariNodes showLogs={showLogs} />
        <MinotariWallet showLogs={showLogs} />
        <ShowInfo executable={Executable.Miner} name="miner" logs={logs?.miner}
                  stdoutLogs={stdoutLogs?.miner} showLogs={showLogs} horizontal={horizontal}>
          <button onClick={() => jsonRpc("mine", { num_blocks: 1 })}>Mine</button>
          <h3>Periodic Mining</h3>
          <div>
            <input type="number" placeholder="Mining interval" disabled={isMining}
                   onChange={(e) => setMiningInterval(Number(e.target.value))}
                   value={miningInterval} />sec/block
          </div>
          <br />
          <button
            onClick={() => jsonRpc("start_mining", { interval_seconds: miningInterval }).then((_) => getInfo())}
            disabled={isMining}>
            Start Mining
          </button>
          <button onClick={() => jsonRpc("stop_mining", {}).then((_) => getInfo())}
                  disabled={!isMining}>Stop Mining
          </button>
        </ShowInfo>
      </div>
      <div>
        <div className="label">Validator Nodes</div>
        <ShowInfos nodes={vns} logs={logs} stdoutLogs={stdoutLogs} name={"vn"} showLogs={showLogs}
                   autoRefresh={autoRefresh} horizontal={horizontal} onReload={getInfo} />
      </div>
      <div>
        <div className="label">Dan Wallets</div>
        <ShowInfos nodes={danWallet} logs={logs} stdoutLogs={stdoutLogs} name={"dan"} showLogs={showLogs}
                   autoRefresh={autoRefresh} horizontal={horizontal} onReload={getInfo} />
      </div>
      <div>
        <div className="label">Indexers</div>
        <ShowInfos nodes={indexers} logs={logs} stdoutLogs={stdoutLogs} name={"indexer"} showLogs={showLogs}
                   autoRefresh={autoRefresh} horizontal={horizontal} onReload={getInfo} />
      </div>
      <div className="label">Templates</div>
      <div className="infos">
        <ShowInfo executable={Executable.Templates} horizontal={horizontal}>
          <input type="file" onChange={handleFileChange} />
          <button onClick={handleFileUpload}>Upload template</button>
        </ShowInfo>
      </div>
      {connectorSample && (
        <div className="label">
          <a href={connectorSample}>Connector sample</a>
        </div>
      )}
      <div className="label">All Instances</div>
      <div>
        <table>
          <thead>
          <tr>
            <td>Name</td>
            <td>Ports</td>
            <td>Base Path</td>
          </tr>
          </thead>
          <tbody>
          {instances.filter((i: any) => i.is_running).map((instance: any, i: number) => <tr key={i}>
            <td>#{instance.id} {instance.name} ({instance.instance_type})</td>
            <td>{JSON.stringify(instance.ports)}</td>
            <td>{instance.base_path}</td>
          </tr>)}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function getDecision(tx: any): string {
  if (!tx) {
    return "-";
  }
  const decision = tx.local_decision || tx.original_decision;

  if (typeof decision === "string") {
    return decision;
  }

  if (typeof decision !== "object") {
    return JSON.stringify(decision);
  }

  if ("Abort" in decision) {
    return "Abort(" + decision.Abort + ")";
  }

  return "Commit";
}