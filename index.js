const fs = require('fs');
const readline = require('readline');
const { Heap } = require('heap-js');

//Укажите желаемые размер временных файлов
const chunkSize = 100 * 1024 * 1024;
const inputFilePath = 'input.txt';
const tempChunkPrefix = 'chunk_';

async function splitAndSortFile() {
    let chunkIndex = 0;
    let lines = [];

    const readStream = fs.createReadStream(inputFilePath);
    const rl = readline.createInterface({
        input: readStream,
        crlfDelay: Infinity
    });

    for await (const line of rl) {
        lines.push(line);
        if (Buffer.byteLength(lines.join('\n'), 'utf8') > chunkSize) {
            lines.sort();
            const tempFileName = tempChunkPrefix + chunkIndex + '.txt';
            fs.writeFileSync(tempFileName, lines.join('\n'));
            chunkIndex++;
            lines = [];
        }
    }

    if (lines.length > 0) {
        lines.sort();
        const tempFileName = tempChunkPrefix + chunkIndex + '.txt';
        fs.writeFileSync(tempFileName, lines.join('\n'));
    }

    const sortedChunks = [];

    for (let i = 0; i < chunkIndex; i++) {
        const chunkFileName = tempChunkPrefix + i + '.txt';
        sortedChunks.push(chunkFileName);
    }

    await mergeSortedChunks(sortedChunks);
}

async function mergeSortedChunks(sortedChunks) {
    const readers = sortedChunks.map((file) => readline.createInterface({
        input: fs.createReadStream(file),
        crlfDelay: Infinity
    }));
    const outputStream = fs.createWriteStream("output.txt");
    const newHeap = new Heap();


    const firstLines = await Promise.all(readers.map(async (reader) => {
        return new Promise((resolve) => {
            reader.once('line', (line) => {
                resolve(line);
            });
        });
    }));

    firstLines.forEach((line, index) => {
        if (line !== undefined) {
            newHeap.push({value: line, reader: readers[index], fileIndex: index});
        }
    });

    while (newHeap.size() > 0) {
        const {value, reader, fileIndex} = newHeap.pop();

        outputStream.write(`${value}\n`);

        const nextLine = await new Promise((resolve) => {
            reader.once('line', (line) => {
                resolve(line);
            });
        });

        if (nextLine !== undefined) {
            newHeap.push({value: nextLine, reader, fileIndex});
        } else {
            reader.close();
            let allReadersClosed = true;
            for (const reader of readers) {
                if (!reader.closed) {
                    allReadersClosed = false;
                    break;
                }
            }
            if (allReadersClosed) {
                outputStream.end();

                for (const chunkFile of sortedChunks) {
                    fs.unlinkSync(chunkFile);
                }
            }
        }
    }
}

splitAndSortFile();