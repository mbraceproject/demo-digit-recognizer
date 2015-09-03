#load "credentials.fsx"
#I "../../bin"
#r "Streams.Core.dll"
#r "DigitRecognizer.dll"
#time "on"

open MBrace.Core
open MBrace.Library
open MBrace.Azure

open Nessos.Streams

open DigitRecognizer
open DigitRecognizer.Knn

// First connect to the cluster
let cluster = MBraceCluster.GetHandle(config, logger = new ConsoleLogger(), logLevel = LogLevel.Debug)

// initialize a local standalone cluster
// let cluster = MBraceCluster.InitOnCurrentMachine(config, workerCount = 2)

let trainPath = __SOURCE_DIRECTORY__ + "/../../data/train.csv"
let testPath = __SOURCE_DIRECTORY__ + "/../../data/test.csv"

// parse data
let training = TrainingImage.Parse trainPath
let tests = Image.Parse testPath

// distributed validation workflow
let validateDistributed (classifier : Classifier) (validation : TrainingImage []) = cloud {
    let! successful =
        validation
        |> Balanced.reduceCombine (fun vs -> local { return validateLocalMulticore classifier training vs })
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

// test: try running a classification cloud task
let classifyTask = cluster.CreateCloudTask (cloud { return! classifyDistributed (knn l2 10) tests })

classifyTask.ShowInfo()
cluster.ShowWorkerInfo()
cluster.ShowCloudTaskInfo()
classifyTask.AwaitResult()