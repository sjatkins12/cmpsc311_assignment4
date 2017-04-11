////////////////////////////////////////////////////////////////////////////////
//
//  File           : crud_file_io_helper.c
//  Description    : Finished functions used for helping new functions
//  Author         : Sam Atkins
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

CrudFileAllocationType openFile;

////////////////////////////////////////////////////////////////////////////////
//
// Function     : crud_open
// Description  : This function opens the file and returns a file handle
//
// Inputs       : path - the path "in the storage array"
// Outputs      : file handle if successful, -1 if failure

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



