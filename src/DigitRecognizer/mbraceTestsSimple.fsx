#load "credentials.fsx"
#I "../../bin"
#r "Streams.Core.dll"
#r "DigitRecognizer.dll"
#time "on"

open MBrace.Core
open MBrace.Workflows
open MBrace.Azure.Client

open Nessos.Streams

open DigitRecognizer
open DigitRecognizer.Knn

// First connect to the cluster
let cluster = Runtime.GetHandle(config)
cluster.AttachClientLogger(new MBrace.Azure.ConsoleLogger())

// initialize a local standalone cluster
// let cluster = Runtime.InitLocal(config, workerCount = 2)

// attach a local worker to cluster
// cluster.AttachLocalWorker()

let trainPath = __SOURCE_DIRECTORY__ + "/../../data/train.csv"
let testPath = __SOURCE_DIRECTORY__ + "/../../data/test.csv"

// parse data
let training = TrainingImage.Parse trainPath
let tests = Image.Parse testPath

// distributed validation workflow
let validateDistributed (classifier : Classifier) (validation : TrainingImage []) = cloud {
    let! successful = 
        validation 
        |> Balanced.reduceCombine 
                (fun validation ->  local { return validateLocalMulticore classifier training validation})
                (fun cs -> local { return Array.sum cs })

    return float successful / float validation.Length
}

// distributed classification workflow
let classifyDistributed (classifier : Classifier) (images : Image []) = cloud {
    return!
        images 
        |> Balanced.reduceCombine 
            (fun images -> local { return classifyLocalMulticore classifier training images }) 
            (fun cs -> local { return Array.concat cs })
}

// test: try running a classification job
let classifyJob = cluster.CreateProcess (cloud { return! classifyDistributed (knn l2 10) tests })

classifyJob.ShowInfo()
cluster.ShowWorkers()
cluster.ShowProcesses()
classifyJob.AwaitResult()