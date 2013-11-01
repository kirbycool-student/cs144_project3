package edu.ucla.cs.cs144;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;

public class Indexer {
    
    /** Creates a new instance of Indexer */
    public Indexer() {
    }
    
	private IndexWriter indexWriter = null;
	    
	@SuppressWarnings("deprecation")
	public IndexWriter getIndexWriter(boolean create) throws IOException {
		if (indexWriter == null) {
			indexWriter = new IndexWriter("index-directory",
                                          new StandardAnalyzer(),
                                          create);
        }
        return indexWriter;
    }
    
 
    @SuppressWarnings("deprecation")
	public void rebuildIndexes() throws CorruptIndexException, SQLException, IOException {

        Connection con = null;

        // create a connection to the database to retrieve Items from MySQL
        try {
        	con = DbManager.getConnection(true);
        } catch (SQLException ex) {
        	System.out.println(ex);
        }

		/*
		 * Add your code here to retrieve Items using the connection
		 * and add corresponding entries to your Lucene inverted indexes.
	         *
	         * You will have to use JDBC API to retrieve MySQL data from Java.
	         * Read our tutorial on JDBC if you do not know how to use JDBC.
	         *
	         * You will also have to use Lucene IndexWriter and Document
	         * classes to create an index and populate it with Items data.
	         * Read our tutorial on Lucene as well if you don't know how.
	         *
	         * As part of this development, you may want to add 
	         * new methods and create additional Java classes. 
	         * If you create new classes, make sure that
	         * the classes become part of "edu.ucla.cs.cs144" package
	         * and place your class source files at src/edu/ucla/cs/cs144/.
		 * 
		 */
      
        PreparedStatement selectItems = con.prepareStatement(
        	"SELECT * from Items"
        );
        ResultSet rs = selectItems.executeQuery();
        
        
        //add the items tot he index
        while(rs.next()) {
           	//System.out.println("Indexing item: " + item);
            IndexWriter writer = getIndexWriter(false);
            Document doc = new Document();
            doc.add(new Field("ItemId", rs.getString("ItemId"), Field.Store.YES, Field.Index.NO));
            doc.add(new Field("Name", rs.getString("Name"), Field.Store.YES, Field.Index.TOKENIZED));
            doc.add(new Field("Category", rs.getString("Category"), Field.Store.YES, Field.Index.TOKENIZED));
            doc.add(new Field("Description", rs.getString("Description"), Field.Store.YES, Field.Index.TOKENIZED));
            String fullSearchableText = rs.getString("Name") + " " + rs.getString("Category") + " " + rs.getString("Description");
            doc.add(new Field("Content", fullSearchableText, Field.Store.NO, Field.Index.TOKENIZED));
            writer.addDocument(doc);
        }
	
	
	        // close the database connection
		try {
		    conn.close();
		} catch (SQLException ex) {
		    System.out.println(ex);
		}
    }    

    public static void main(String args[]) {
        Indexer idx = new Indexer();
        idx.rebuildIndexes();
    }   
}
