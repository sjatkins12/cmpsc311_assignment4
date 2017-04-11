////////////////////////////////////////////////////////////////////////////////
//
//  File           : crud_file_io.h
//  Description    : This is the implementation of the standardized IO functions
//                   for used to access the CRUD storage system.
//
//  Author         : Patrick McDaniel
//  Last Modified  : Mon Oct 20 12:38:05 PDT 2014
//

// Includes
#include <malloc.h>
#include <string.h>

// Project Includes
#include <crud_file_io.h>
#include <cmpsc311_log.h>
#include <cmpsc311_util.h>

// Defines
#define CIO_UNIT_TEST_MAX_WRITE_SIZE 1024
#define CRUD_IO_UNIT_TEST_ITERATIONS 10240

// Other definitions

// Type for UNIT test interface
typedef enum {
	CIO_UNIT_TEST_READ   = 0,
	CIO_UNIT_TEST_WRITE  = 1,
	CIO_UNIT_TEST_APPEND = 2,
	CIO_UNIT_TEST_SEEK   = 3,
} CRUD_UNIT_TEST_TYPE;

// File system Static Data
// This the definition of the file table
CrudFileAllocationType crud_file_table[CRUD_MAX_TOTAL_FILES]; // The file handle table

// Pick up these definitions from the unit test of the crud driver
CrudRequest construct_crud_request(CrudOID oid, CRUD_REQUEST_TYPES req,
		uint32_t length, uint8_t flags, uint8_t res);
int deconstruct_crud_request(CrudRequest request, CrudOID *oid,
		CRUD_REQUEST_TYPES *req, uint32_t *length, uint8_t *flags,
		uint8_t *res);

//
// Implementation
int16_t crud_open(char *path) {
	CrudResponse response;
	CrudRequest request = CRUD_INIT;
	char *buff;

	buff = malloc(CRUD_MAX_OBJECT_SIZE);
	response = crud_bus_request(request, NULL); // Initialize Object Store
	request = CRUD_CREATE;

	request <<= 24; //Move to correct position in array 
	request += 0;
	request <<= 4; // Add ending zeros for flags & return bit
	response = crud_bus_request(request, buff);
	free(buff);
	openFile.OID = response >> 32;
	openFile.length = 0;
	openFile.position = 0;
	openFile.fd = openFile.OID;
	if (response & 0x1) //Sucsessfull CRUD Request
		return (-1); // Failure to create new object
	else
		return (response >> 32); // Return OID as FD


}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_close
// Description  : This function closes the file
//
// Inputs       : fd - the file handle of the object to close
// Outputs      : 0 if successful, -1 if failure

int16_t crud_close(int16_t fh) {
	CrudResponse response;
	CrudRequest request = openFile.OID;

	request <<= 4;
	request += CRUD_DELETE;
	request <<= 28;
	response = crud_bus_request(request, NULL);
	if (response & 0x1) { // Check for good delete
		return (-1);
	}
	else 
		return (0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_read
// Description  : Reads up to "count" bytes from the file handle "fh" into the
//                buffer  "buf".
//
// Inputs       : fd - the file descriptor for the read
//                buf - the buffer to place the bytes into
//                count - the number of bytes to read
// Outputs      : the number of bytes read or -1 if failures

int32_t crud_read(int16_t fd, void *buf, int32_t count) {
	CrudResponse response;
	CrudRequest request = openFile.OID;
	int i = 0;
	char *tbuf;
	
	tbuf = malloc(openFile.length); //Size of object
	request <<= 4;
	request += CRUD_READ;
	request <<= 24;
	request += openFile.length;
	request <<= 4;
	response = crud_bus_request(request, tbuf); // Read Entire Object
	if (response & 0x1) { // Check for good read
		free(tbuf);
		return (-1);
	}
	// Count up to then end of the object
	if (openFile.position + count > openFile.length)
		count = openFile.length - openFile.position;
	memcpy(buf, &tbuf[openFile.position], count); // Copy Read data into buf
	free(tbuf);
	openFile.position += count; // UPdate pos
	return (count);
}

//////////////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_write
// Description  : Writes "count" bytes to the file handle "fh" from the
//                buffer  "buf"
//
// Inputs       : fd - the file descriptor for the file to write to
//                buf - the buffer to write
//                count - the number of bytes to write
// Outputs      : the number of bytes written or -1 if failure

int32_t crud_write(int16_t fd, void *buf, int32_t count) {
	CrudResponse response;
	CrudRequest request = openFile.OID;
	char *tbuf;
	char *cbuf;

	// READ ALL OF OBJECT INTO CBUF
	request = openFile.OID;
	request <<= 4;
	request += CRUD_READ;
	request <<= 24;
	request += openFile.length;
	request <<= 4;
	cbuf = malloc(openFile.length);
	response = crud_bus_request(request, cbuf);
	if (response & 0x1) { //MAKE SURE GOOD READ
		free(cbuf);
		return (-1);
	}
	// Write to big for current Object
	if (openFile.position + count > openFile.length) { 
		// DELETE OLD OBJECT
		request = openFile.OID;
		request <<= 4;
		request += CRUD_DELETE;
		request <<= 28;
		response = crud_bus_request(request, buf);
		if (response & 0x1) { //MAKE SURE GOOD DELETE
		free(cbuf);
		return (-1);
		}
		tbuf = malloc(openFile.position + count);
		// TBUF DATA FOR NEW OBJECT
		memcpy(tbuf, cbuf, openFile.length);
		free(cbuf);
		memcpy(&tbuf[openFile.position], buf, count);
		// CREATE NEW OBJECT
		request += CRUD_CREATE;
		request <<= 24;
		request += openFile.position + count;
		request <<= 4;
		response = crud_bus_request(request, tbuf);
		free(tbuf);
		if (response & 0x1) { //MAKE SURE GOOD CREATE
			
			return (-1);
		}
		openFile.OID = (response >> 32); // Save new OID
		openFile.length = openFile.position + count; //Update length
		openFile.position += count; //Update pos
		return (count);
	}
	else { //Object is large enough for write
		memcpy(&cbuf[openFile.position], buf, count); //Copy new data into cbuf
		//Update Object with new buf
		request = openFile.OID;
		request <<= 4;
		request += CRUD_UPDATE;
		request <<= 24;
		request += openFile.length;
		request <<= 4;
		response = crud_bus_request(request, cbuf);
		if (response & 0x1) { //MAKE SURE GOOD UPDATE
			free(cbuf);
			return (-1);
		}
		openFile.position += count;
		free(cbuf);
		return (count);
	}
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_seek
// Description  : Seek to specific point in the file
//
// Inputs       : fd - the file descriptor for the file to seek
//                loc - offset from beginning of file to seek to
// Outputs      : 0 if successful or -1 if failure

int32_t crud_seek(int16_t fd, uint32_t loc) {
	openFile.position = loc; //Update Position 
	return (0);
}
////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_format
// Description  : This function formats the crud drive, and adds the file
//                allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_format(void) {

	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... formatting complete.");
	return(0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_mount
// Description  : This function mount the current crud file system and loads
//                the file allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_mount(void) {

	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... mount complete.");
	return(0);
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_unmount
// Description  : This function unmounts the current crud file system and
//                saves the file allocation table.
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

uint16_t crud_unmount(void) {

	// Log, return successfully
	logMessage(LOG_INFO_LEVEL, "... unmount complete.");
	return (0);
}

// *** INSERT YOUR CODE HERE ***

// Module local methods

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crudIOUnitTest
// Description  : Perform a test of the CRUD IO implementation
//
// Inputs       : None
// Outputs      : 0 if successful or -1 if failure

int crudIOUnitTest(void) {

	// Local variables
	uint8_t ch;
	int16_t fh, i;
	int32_t cio_utest_length, cio_utest_position, count, bytes, expected;
	char *cio_utest_buffer, *tbuf;
	CRUD_UNIT_TEST_TYPE cmd;
	char lstr[1024];

	// Setup some operating buffers, zero out the mirrored file contents
	cio_utest_buffer = malloc(CRUD_MAX_OBJECT_SIZE);
	tbuf = malloc(CRUD_MAX_OBJECT_SIZE);
	memset(cio_utest_buffer, 0x0, CRUD_MAX_OBJECT_SIZE);
	cio_utest_length = 0;
	cio_utest_position = 0;

	// Format and mount the file system
	if (crud_format() || crud_mount()) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure on format or mount operation.");
		return(-1);
	}

	// Start by opening a file
	fh = crud_open("temp_file.txt");
	if (fh == -1) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure open operation.");
		return(-1);
	}

	// Now do a bunch of operations
	for (i=0; i<CRUD_IO_UNIT_TEST_ITERATIONS; i++) {

		// Pick a random command
		if (cio_utest_length == 0) {
			cmd = CIO_UNIT_TEST_WRITE;
		} else {
			cmd = getRandomValue(CIO_UNIT_TEST_READ, CIO_UNIT_TEST_SEEK);
		}

		// Execute the command
		switch (cmd) {

		case CIO_UNIT_TEST_READ: // read a random set of data
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : read %d at position %d", bytes, cio_utest_position);
			bytes = crud_read(fh, tbuf, count);
			if (bytes == -1) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Read failure.");
				return(-1);
			}

			// Compare to what we expected
			if (cio_utest_position+count > cio_utest_length) {
				expected = cio_utest_length-cio_utest_position;
			} else {
				expected = count;
			}
			if (bytes != expected) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : short/long read of [%d!=%d]", bytes, expected);
				return(-1);
			}
			if ( (bytes > 0) && (memcmp(&cio_utest_buffer[cio_utest_position], tbuf, bytes)) ) {

				bufToString((unsigned char *)tbuf, bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST R: %s", lstr);
				bufToString((unsigned char *)&cio_utest_buffer[cio_utest_position], bytes, (unsigned char *)lstr, 1024 );
				logMessage(LOG_INFO_LEVEL, "CIO_UTEST U: %s", lstr);

				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : read data mismatch (%d)", bytes);
				return(-1);
			}
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : read %d match", bytes);


			// update the position pointer
			cio_utest_position += bytes;
			break;

		case CIO_UNIT_TEST_APPEND: // Append data onto the end of the file
			// Create random block, check to make sure that the write is not too large
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			if (cio_utest_length+count >= CRUD_MAX_OBJECT_SIZE) {

				// Log, seek to end of file, create random value
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : append of %d bytes [%x]", count, ch);
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : seek to position %d", cio_utest_length);
				if (crud_seek(fh, cio_utest_length)) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : seek failed [%d].", cio_utest_length);
					return(-1);
				}
				cio_utest_position = cio_utest_length;
				memset(&cio_utest_buffer[cio_utest_position], ch, count);

				// Now write
				bytes = crud_write(fh, &cio_utest_buffer[cio_utest_position], count);
				if (bytes != count) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : append failed [%d].", count);
					return(-1);
				}
				cio_utest_length = cio_utest_position += bytes;
			}
			break;

		case CIO_UNIT_TEST_WRITE: // Write random block to the file
			ch = getRandomValue(0, 0xff);
			count =  getRandomValue(1, CIO_UNIT_TEST_MAX_WRITE_SIZE);
			// Check to make sure that the write is not too large
			if (cio_utest_length+count < CRUD_MAX_OBJECT_SIZE) {
				// Log the write, perform it
				logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : write of %d bytes [%x]", count, ch);
				memset(&cio_utest_buffer[cio_utest_position], ch, count);
				bytes = crud_write(fh, &cio_utest_buffer[cio_utest_position], count);
				if (bytes!=count) {
					logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : write failed [%d].", count);
					return(-1);
				}
				cio_utest_position += bytes;
				if (cio_utest_position > cio_utest_length) {
					cio_utest_length = cio_utest_position;
				}
			}
			break;

		case CIO_UNIT_TEST_SEEK:
			count = getRandomValue(0, cio_utest_length);
			logMessage(LOG_INFO_LEVEL, "CRUD_IO_UNIT_TEST : seek to position %d", count);
			if (crud_seek(fh, count)) {
				logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : seek failed [%d].", count);
				return(-1);
			}
			cio_utest_position = count;
			break;

		default: // This should never happen
			CMPSC_ASSERT0(0, "CRUD_IO_UNIT_TEST : illegal test command.");
			break;

		}

#if DEEP_DEBUG
		// VALIDATION STEP: ENSURE OUR LOCAL IS LIKE OBJECT STORE
		CrudRequest request;
		CrudResponse response;
		CrudOID oid;
		CRUD_REQUEST_TYPES req;
		uint32_t length;
		uint8_t res, flags;

		// Make a fake request to get file handle, then check it
		request = construct_crud_request(crud_file_table[0].object_id, CRUD_READ, CRUD_MAX_OBJECT_SIZE, CRUD_NULL_FLAG, 0);
		response = crud_bus_request(request, tbuf);
		if ((deconstruct_crud_request(response, &oid, &req, &length, &flags, &res) != 0) || (res != 0))  {
			logMessage(LOG_ERROR_LEVEL, "Read failure, bad CRUD response [%x]", response);
			return(-1);
		}
		if ( (cio_utest_length != length) || (memcmp(cio_utest_buffer, tbuf, length)) ) {
			logMessage(LOG_ERROR_LEVEL, "Buffer/Object cross validation failed [%x]", response);
			bufToString((unsigned char *)tbuf, length, (unsigned char *)lstr, 1024 );
			logMessage(LOG_INFO_LEVEL, "CIO_UTEST VR: %s", lstr);
			bufToString((unsigned char *)cio_utest_buffer, length, (unsigned char *)lstr, 1024 );
			logMessage(LOG_INFO_LEVEL, "CIO_UTEST VU: %s", lstr);
			return(-1);
		}

		// Print out the buffer
		bufToString((unsigned char *)cio_utest_buffer, cio_utest_length, (unsigned char *)lstr, 1024 );
		logMessage(LOG_INFO_LEVEL, "CIO_UTEST: %s", lstr);
#endif

	}

	// Close the files and cleanup buffers, assert on failure
	if (crud_close(fh)) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure read comparison block.", fh);
		return(-1);
	}
	free(cio_utest_buffer);
	free(tbuf);

	// Format and mount the file system
	if (crud_unmount()) {
		logMessage(LOG_ERROR_LEVEL, "CRUD_IO_UNIT_TEST : Failure on unmount operation.");
		return(-1);
	}

	// Return successfully
	return(0);
}

































