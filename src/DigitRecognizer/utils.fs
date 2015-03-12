[<AutoOpen>]
module DigitRecognizer.Utils

open System
open System.IO
open System.Text
open Nessos.Streams

type Stream =
    // quick'n'dirty implementation of missing Stream combinator
    /// Takes a System.IO.Stream returning a Nessos.Stream<string> containing all lines of text
    static member OfTextStream(stream : System.IO.Stream, ?encoding : Encoding) : Stream<string> =
        seq { 
            use sr = match encoding with None -> new StreamReader(stream) | Some e -> new StreamReader(stream, e)
            while not sr.EndOfStream do yield sr.ReadLine () }
        |> Stream.ofSeq


type TrainingImage with

    /// Parses a training set from text using the Kaggle digit recognizer CSV format
    static member Parse(stream : System.IO.Stream, ?encoding : Encoding) : TrainingImage [] =
        Stream.OfTextStream(stream, ?encoding = encoding)
        |> Stream.skip 1
        |> Stream.map (fun line -> line.Split(','))
        |> Stream.map (fun line -> line |> Array.map int)
        |> Stream.mapi (fun i nums -> 
                            let id = i + 1
                            let image = { Id = id ; Pixels = nums.[1..] }
                            { Classification = nums.[0] ; Image = image })
        |> Stream.toArray

    /// Parses a training set from text using the Kaggle digit recognizer CSV format
    static member Parse(file : string, ?encoding : Encoding) : TrainingImage [] =
        use fs = File.OpenRead file
        TrainingImage.Parse(fs)


type Image with

    /// Parses a set of points from text using the Kaggle digit recognizer CSV format
    static member Parse (stream : System.IO.Stream, ?encoding : Encoding) : Image [] =
        Stream.OfTextStream(stream, ?encoding = encoding)
        |> Stream.skip 1
        |> Stream.map (fun line -> line.Split(','))
        |> Stream.map (fun line -> line |> Array.map int)
        |> Stream.mapi (fun i nums -> let id = i + 1 in { Id = id ; Pixels = nums })
        |> Stream.toArray

    /// Parses a set of points from text using the Kaggle digit recognizer CSV format
    static member Parse (file : string, ?encoding : Encoding) : Image [] =
        use fs = File.OpenRead file
        Image.Parse fs


type Classifications =

    /// Writes a point classification to stream
    static member Write(stream : System.IO.Stream, classifications : (ImageId * Classification) [], ?encoding : Encoding) =
        use sw = match encoding with None -> new StreamWriter(stream) | Some e -> new StreamWriter(stream, e)
        sw.WriteLine "ImageId,Label"
        classifications |> Array.iter (fun (i,c) -> sw.WriteLine(sprintf "%d,%d" i c))

    /// Writes a point classification to stream
    static member Write(outFile : string, classifications : (ImageId * Classification) [], ?encoding : Encoding) =
        use fs = File.OpenWrite(outFile)
        Classifications.Write(fs, classifications, ?encoding = encoding)