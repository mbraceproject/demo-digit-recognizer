﻿(*
Replicating the Naive Baseline, running computations on mbrace.
"Simplest possible model": 1-nearest neighbor, euclidean distance.
*)

(*
Base types: 
* an Image is an array of 28 * 28 grayscale pixels (0..255)
* an Example is an image, with known Label (0 to 9)
* a Benchmark is an image we try to predict, with an ID but no Label
*)

type Image = int []
type Example = { Label: int; Image: Image }
type Benchmark = { ImageId: int; Image: Image }


(*
Connect to Cluster
*)

#load "credentials.fsx"
#r "System.Runtime.Caching.dll"

open MBrace
open MBrace.Store
open MBrace.Workflows
open MBrace.Azure.Client
open System.IO
open System.Runtime.Caching

let cluster = Runtime.GetHandle(config)
cluster.AttachClientLogger(new MBrace.Azure.ConsoleLogger())
cluster.ShowWorkers()

let logInfo message =
    local {
        let! worker = Cloud.CurrentWorker
        return! Cloud.Log <| sprintf "%s: %s" worker.Id message
    }

let trainFileName = "train.csv"
let testFileName = "test.csv"
let submissionFileName = "submission.csv"

let localPath (fileName:string) =
    let localDataFolder = __SOURCE_DIRECTORY__ + "/../../data/"
    localDataFolder + fileName

// move csv files to cluster storage
let mbraceDataFolder = cluster.StoreClient.FileStore.File
   
let upload (fileName:string) =
    match mbraceDataFolder.Enumerate() |> Seq.tryFind(fun file -> file.Path.Contains fileName) with
    | Some file -> file
    | None -> mbraceDataFolder.Upload (localPath fileName)
    
let cloudTrain = upload trainFileName
let cloudTest = upload testFileName

// Reading the 50,000 known examples in memory.
let predict (trainingSet:Example []) =
    // Euclidean distance between 2 images.
    let size = 28 * 28
    let distance (img1:Image) (img2:Image) =
        let rec compute acc i =
            if i = size then acc
            else
                let d = (img1.[i] - img2.[i]) * (img1.[i] - img2.[i])
                compute (acc + d) (i + 1)
        compute 0 0

    // Given an image, find the label of the closest
    // image from the 50,000 known training images.
    fun img ->
        trainingSet
        |> Seq.minBy (fun example -> distance example.Image img)
        |> fun closest -> closest.Label


/// Some helper functions for caching data across the cluster.
module Caching =
    /// Caches a CloudFile across the cluster, with an optional handler to preprocess the data before insertion.
    let private CacheAcrossCluster cacheName handler (cloudFile:CloudFile) =
        local {
            if not (MemoryCache.Default.Contains cacheName) then
                do! logInfo <| sprintf "Adding %s to in-memory cache." cacheName
                let! lines = cloudFile.Path |> CloudFile.ReadAllLines
                let cacheableObject = handler lines
                return (MemoryCache.Default.Add(cacheName, cacheableObject, CacheItemPolicy()) |> ignore)
            else return()
        } |> Cloud.ParallelEverywhere |> Cloud.Ignore
    
    let GetTrainingSet() = MemoryCache.Default.[trainFileName] :?> Example[]
    let GetBenchmarkSet() = MemoryCache.Default.[testFileName] :?> Benchmark[]
    let CacheExampleFile() =
        let handler (lines:string array) =
            lines.[1..]
            |> Array.map (fun line -> line.Split ',' |> Array.map int)
            |> Array.map (fun line -> { Label = line.[0]; Image = line.[1..] })
        cloudTrain |> CacheAcrossCluster trainFileName handler
    
    let CacheBenchmarkFile() =
        let handler (lines:string array) =
            lines.[1..]
            |> Array.map (fun line -> line.Split ',' |> Array.map int)
            |> Array.mapi (fun i image -> { ImageId = i + 1; Image = image } )
        cloudTest |> CacheAcrossCluster testFileName handler
    
    let Clear() =
        local {
            [ testFileName; trainFileName ]
            |> List.map MemoryCache.Default.Remove
            |> ignore
        } |> Cloud.ParallelEverywhere |> cluster.Run

module JobSubmission =
    /// Creates a submission using a supplied prediction function
    let CreateSubmission predictionFunction =
        cloud {
            do! logInfo "Priming caches..."
            do! Caching.CacheExampleFile()
            do! Caching.CacheBenchmarkFile()
    
            let benchmark = Caching.GetBenchmarkSet()            
            do! logInfo "Starting processing..."
            return!
                benchmark
                |> DivideAndConquer.map(fun test ->
                    local { 
                        let trainingSet = Caching.GetTrainingSet()
                        let (prediction:int) = predictionFunction trainingSet test.Image
                        return test.ImageId, prediction }) }
    
    /// Saves a submission file for the supplied predictions.
    let SaveSubmission outfile predictions =
        let lines =
            "ImageId,Label"
            :: (predictions
                |> Seq.map(fun (rowId, value) -> sprintf "%O,%O" rowId value)
                |> Seq.toList)
            |> Seq.toArray
        File.WriteAllLines(outfile, lines)
    
    /// Orchestrates a job to calculate the predictions and saves the results on completion.
    let PredictAndSave predictionFunction outfile =
        let job = CreateSubmission predictionFunction |> cluster.CreateProcess
        async {
            let! result = job.AwaitResultAsync()
            result |> SaveSubmission outfile
        } |> Async.Start
        job

let submission = JobSubmission.PredictAndSave predict @"C:\users\isaac\desktop\kaggle.csv"
submission.ShowInfo()
submission.ShowLogs()
cluster.ShowWorkers()
submission.AwaitResult()
