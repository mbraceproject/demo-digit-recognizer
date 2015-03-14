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

// distributed classification workflow
let classifyDistributed (classifier : Classifier) (trainingRef : CloudSequence<TrainingImage>) (images : Image []) = cloud {
    let evaluateSingleThreaded (images : Image []) = local {
        let! _ = trainingRef.PopulateCache() // cache to local memory for future use
        let! training = trainingRef.ToArray()
        return images |> Array.map (fun img -> img.Id, classifier training img)
    }

    let! successful = images |> DivideAndConquer.reduceCombine evaluateSingleThreaded (Local.lift Array.concat)
    return successful
}

// warmup: force in-memory caching of entities in cloud
let cache() = cloud {
    let! s1 = cloudTraining.PopulateCache()
    let! s2 = cloudTest.PopulateCache()
    return s1 && s2
}

let cacheJob = cluster.CreateProcess(Cloud.ParallelEverywhere(cache()))
cacheJob.ShowInfo()


// test: try running a classification job
cluster.Run(
    cloud {
        let! images = cloudTest.ToArray()
        return! classifyDistributed (knn l2 10) cloudTraining images
    })