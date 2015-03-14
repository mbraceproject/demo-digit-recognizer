#load "credentials.fsx"
#I "../../bin"
#r "Streams.Core.dll"
#r "DigitRecognizer.dll"

open MBrace
open MBrace.Store
open MBrace.Workflows
open MBrace.Azure.Client

open Nessos.Streams

open DigitRecognizer
open DigitRecognizer.Knn

let trainPath = __SOURCE_DIRECTORY__ + "/../../data/train.csv"
let testPath = __SOURCE_DIRECTORY__ + "/../../data/test.csv"

let training = TrainingImage.Parse trainPath

let training' = training.[ .. 39999]
let validation = training.[40000 ..]

// First connect to the cluster
let cluster = Runtime.GetHandle(config)

// Save training set as cloud reference in Azure store, returning a typed reference to data
let trainingRef = cluster.DefaultStoreClient.CloudSequence.New training'

let evaluateDistributed (classifier : Classifier) (training : CloudSequence<TrainingImage>) (validation : TrainingImage []) = cloud {
    let evaluateSingleThreaded (validation : TrainingImage []) = local {
        let! _ = trainingRef.PopulateCache() // cache to local memory for future use
        let! training = trainingRef.ToArray()
        return
            validation
            |> Stream.ofArray
            |> Stream.filter (fun timg -> timg.Classification = classifier training timg.Image)
            |> Stream.length
    }

    let! successful = validation |> DivideAndConquer.reduceCombine evaluateSingleThreaded (Local.lift Array.sum)
    return float successful / float validation.Length
}

let job = evaluateDistributed (knn l2 10) trainingRef validation |> cluster.CreateProcess

job.AwaitResult()