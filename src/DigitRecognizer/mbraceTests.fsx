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

// First connect to the cluster
let cluster = Runtime.GetHandle(config)
cluster.AttachClientLogger(new MBrace.Azure.ConsoleLogger())
// let cluster = Runtime.InitLocal(config, workerCount = 2)


// use zipped .csv files
let trainPathGz = __SOURCE_DIRECTORY__ + "/../../data/train.csv.gz"
let testPathGz = __SOURCE_DIRECTORY__ + "/../../data/test.csv.gz"

// upload to store; expect ~30sec for each file
let cloudTrainGz = cluster.DefaultStoreClient.FileStore.File.Upload trainPathGz
let cloudTestGz = cluster.DefaultStoreClient.FileStore.File.Upload testPathGz

// create a lazy, distributed reference to the data by attaching a deserialize function to the cloud file
let cloudTraining = cluster.RunLocal(TrainingImage.Parse(cloudTrainGz, decompress = true))
let cloudTest = cluster.RunLocal(Image.Parse(cloudTestGz, decompress = true))

// test entities
cloudTraining.ToEnumerable() |> cluster.RunLocal |> Seq.take 10 |> Seq.toArray
cloudTest.ToEnumerable() |> cluster.RunLocal |> Seq.take 10 |> Seq.toArray

cluster.Run(local { let! seq = cloudTraining.ToEnumerable() in return Seq.take 10 seq |> Seq.toArray })

// distributed validation workflow
let validateDistributed (classifier : Classifier) (trainingRef : CloudSequence<TrainingImage>) (validation : TrainingImage []) = cloud {
    let validateLocal (validation : TrainingImage []) = local {
        let! _ = trainingRef.PopulateCache() // cache to local memory for future use
        let! training = trainingRef.ToArray()
        return validateLocalMulticore classifier training validation
    }

    let! successful = validation |> Balanced.reduceCombine validateLocal (fun cs -> local { return Array.sum cs })
    return float successful / float validation.Length
}

// distributed classification workflow
let classifyDistributed (classifier : Classifier) (trainingRef : CloudSequence<TrainingImage>) (images : Image []) = cloud {
    let evaluateSingleThreaded (images : Image []) = local {
        let! _ = trainingRef.PopulateCache() // cache to local memory for future use
        let! training = trainingRef.ToArray()
        return classifyLocalMulticore classifier training images
    }

    let! successful = images |> Balanced.reduceCombine evaluateSingleThreaded (fun cs -> local { return Array.concat cs })
    return successful
}

// warmup: force in-memory caching of entities in cloud
let cache () = 
    cloud {
        let! s1 = cloudTraining.PopulateCache()
        let! s2 = cloudTest.PopulateCache()
        return s1 && s2
    } |> Cloud.ParallelEverywhere

let cacheJob = cluster.CreateProcess(cache())
cacheJob.ShowInfo()
cacheJob.AwaitResult()
cluster.ShowLogs()

// test: try running a classification job
let classifyJob =
    cloud {
        let! images = cloudTest.ToArray()
        return! classifyDistributed (knn l2 10) cloudTraining images
    } |> cluster.CreateProcess

classifyJob.ShowInfo()
cluster.ShowWorkers()
classifyJob.AwaitResult()