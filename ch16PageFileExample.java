import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;


/**
 *
 * @author Chris Irwin Davis
 * @version 1.0
 */
public class ch16PageFileExample {
	/*
	 *  Define the page size in bytes
	 * The size of the page file will always be increments of this values
	 */
	static int pageSize = 512;
	
	public static void main(String[] args) {

		/* Create a binary table file. An actual NEW table would begin with a single page and
		 * increase in size only when the first page becomes full.  However, this example 
		 * demonstrates how to add/remove increments of pages to/from files after inserting
		 * only a few records into the first page.
		 *
		 * Note that whenever the length of a RandomAccessFile is increased, the added space
		 * is padded with 0x00 value bytes.
		 */
		String fileName = "students.tbl";
		RandomAccessFile binaryFile;
		try {
			binaryFile = new RandomAccessFile(fileName, "rw");

			/* Initialize the file size to be zero bytes */
			binaryFile.setLength(0);
			/* Display information about the zero-size file */
			System.out.println("The file is now " + binaryFile.length() + " bytes long");
			System.out.println("The file is now " + binaryFile.length() / pageSize + " pages long\n");
			
			/* Increase the file size to be 512B, i.e. One page, 1 x 512B */
			binaryFile.setLength(pageSize * 1);
			System.out.println("The file is now " + binaryFile.length() + " bytes long");
			System.out.println("The file is now " + binaryFile.length() / pageSize + " pages long\n");
			
			
			/* 
			 * Records are fixed size 32 bytes
			 * rowid INT - 4 bytes
			 * name CHAR(24) - 24 bytes
			 * gpa FLOAT - 4 bytes
			 */
			
			int recordSize = 32;
			int recordLocation = 0;
			int currentPage = 0;
			int  pageLocation = pageSize * currentPage;


			/* Write the first record to the beginning of the first page */
			binaryFile.seek(pageLocation + recordLocation); /* Locate the pointer to the beginning of the page */
			binaryFile.writeInt(1);                           // Write rowid=1
			binaryFile.seek(pageLocation + recordLocation + 4);
			binaryFile.writeBytes("Chris Irwin Davis");       // Write name="Chris Irwin Davis" 
			binaryFile.seek(pageLocation + recordLocation + 28);
			binaryFile.writeFloat(3.72F);                     // Write gpa=3.72 

			recordLocation += recordSize;

			/* Write the second record immediately after the first */
			binaryFile.seek(pageLocation + recordLocation); /* Locate the pointer to the beginning of the page */
			binaryFile.writeInt(2);                           // Write rowid=2
			binaryFile.seek(pageLocation + recordLocation + 4);
			binaryFile.writeBytes("John Smith");              // Write name="John Smith" 
			binaryFile.seek(pageLocation + recordLocation + 28);
			binaryFile.writeFloat(3.17F);                     // Write gpa=3.17
			
			recordLocation += recordSize;

			/* Write the third record immediately after the second */
			binaryFile.seek(pageLocation + recordLocation); /* Locate the pointer to the beginning of the page */
			binaryFile.writeInt(3);                           // Write rowid=3
			binaryFile.seek(pageLocation + recordLocation + 4);
			binaryFile.writeBytes("Mary Jones");              // Write name="Mary Jones" 
			binaryFile.seek(pageLocation + recordLocation + 28);
			binaryFile.writeFloat(3.89F);                     // Write gpa=3.89
			
			
			/* 
			 *  Even though the first page isn't full yet... add a new blank page.
			 *  Then continue inserting records at the beginning of the second page.
  			 */
			
			/* Increase the file size to be 1024B, i.e. Two pages, 2 x 512B */
			binaryFile.setLength(pageSize * 2);
			System.out.println("The file is now " + binaryFile.length() + " bytes long");
			System.out.println("The file is now " + binaryFile.length() / pageSize + " pages long");
			System.out.println();


			pageLocation = pageSize * 1;
			recordLocation = 0;
			
			/* Write the fourth record in the _second_ page */
			binaryFile.seek(pageLocation + recordLocation); /* Locate the pointer to the beginning of the 2nd page */
			binaryFile.writeInt(4);                           // Write rowid=4
			binaryFile.seek(pageLocation + recordLocation + 4);
			binaryFile.writeBytes("Cathy Williams");          // Write name="Cathy Williams" 
			binaryFile.seek(pageLocation + recordLocation + 28);
			binaryFile.writeFloat(3.94F);                     // Write gpa=3.89

			/* Increase the file size to be 1536B, i.e. Three pages, 3 x 512B */
			binaryFile.setLength(pageSize * 3);
			System.out.println("The file is now " + binaryFile.length() + " bytes long");
			System.out.println("The file is now " + binaryFile.length() / pageSize + " pages long");
			System.out.println();

			/*
			 *  Increase the size of the binaryFile by exactly one page, regardless of how long it 
			 *  currently is, by getting the current file length, i.e. binaryFile.length(), then
			 *  adding a pageSize number of byte. The new bytes will be appended to the end and be
			 *  all zeros.
			 */
			binaryFile.setLength(binaryFile.length() + pageSize);
			System.out.println("The file is now " + binaryFile.length() + " bytes long");
			System.out.println("The file is now " + binaryFile.length() / pageSize + " pages long");
			System.out.println();
			
			/*
			 *  Decrease the size of the binaryFile by exactly one page, regardless of how long it 
			 *  currently is, by getting the current file length, i.e. binaryFile.length(), then
			 *  subtracting a pageSize number of bytes. A pageSize number of bytes will be removed
			 *  from the highest address, the last page. Any data there will be lost.
			 */
			binaryFile.setLength(binaryFile.length() - pageSize);
			System.out.println("The file is now " + binaryFile.length() + " bytes long");
			System.out.println("The file is now " + binaryFile.length() / pageSize + " pages long");
			System.out.println();
			
			
			displayBinaryHex(binaryFile);
			binaryFile.close();
			
		}
		catch (Exception e) {
			System.out.println("Unable to open " + fileName);
		}

	}
	

	
	/**
	 * <p>This method is used for debugging and file analysis.
	 * @param raf is an instance of {@link RandomAccessFile}. 
	 * <p>This method will display the contents of the file to Stanard Out (stdout)
	 *    as hexadecimal byte values.
	 */
	static void displayBinaryHex(RandomAccessFile raf) {
		try {
			System.out.println("Dec\tHex\t 0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F");
			raf.seek(0);
			long size = raf.length();
			int row = 1;
			System.out.print("0000\t0x0000\t");
			while(raf.getFilePointer() < size) {
				System.out.print(String.format("%02X ", raf.readByte()));
				if(row % 16 == 0) {
					System.out.println();
					System.out.print(String.format("%04d\t0x%04X\t", row, row));
				}
				row++;
			}
		}
		catch (Exception e) {
			System.out.println(e);
		}
	}
}