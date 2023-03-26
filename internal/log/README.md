

Each segement contains two things, a store file and an index file. The store file is where we store the record data; in principle this where we continually append logs. 
The index file is where we index each record in the store. In principle the index file allows us to speed up reads, because it maps record offsets to their position in the store.

This makes reading a record given it's offset a two step process. First in the store file. First you get the entry from the index file, this tells you the position of the record in the store file, then we can read the record in the store file from that position.
The index file is much smaller in comparision the store file, it contains the offset and the stored position of the record.
