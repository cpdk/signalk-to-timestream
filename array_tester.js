
 /*
    * Split array into multiple arrays of max size chunk_size and return array of chunks
    */
 const _array_chunks = function(array, chunk_size) {
    return Array(Math.ceil(array.length / chunk_size))
        .fill()
        .map((_, index) => index * chunk_size)
        .map(begin => array.slice(begin, begin + chunk_size));
};

let testData = [];

for (let i = 0;i < 100;i++) {
    testData.push("Value #" + i);
}

let chunks = _array_chunks(testData, 10);

if (chunks.length != 10) {
    throw new Error('Unexpected number of chunks');
}

testData.push('One more');
chunks = _array_chunks(testData, 10);

if (chunks.length != 11) {
    throw new Error('Unexpected number of chunks');
}

if (chunks[10].length != 1) {
    throw new Error('Unexpected length of last chunk');
}

if (chunks[4][5] != 'Value #45') {
    throw new Error('Unexpected value found in chunk: ' + chunks[4][5]);
}

chunks = _array_chunks(testData, 110);

if (chunks.length != 1) {
    throw new Error('Unexpected number of chunks - only one expected');
}
if (chunks[0].length != 101) {
    throw new Error('Unexpected length of single chunk');
}



console.log('Test completed');