package hadoop.HDFSImplementation;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.common.IOUtils;

public class HadoopFileSystemProgram {

	public static void main(String args[]) throws IOException {
		String uri = args[0];
		String dest = args[1];
		String directoryPath = args[2];
		FileSystem fs = FileSystem.get(URI.create(uri), new Configuration());
		FileStatus fst = fs.getFileStatus(new Path(uri));
		String fileMetadata = fst.getBlockSize() + " " + fst.getPath() + " " + fst.getPermission();
		FSDataInputStream in = null;
		FSDataOutputStream out = fs.create(new Path(dest));
		FileStatus[] stats = fs.listStatus(new Path(directoryPath));
		Path[] listPaths = FileUtil.stat2Paths(stats);

		for (Path p : listPaths) {
			out.writeBytes(p.toString());
		}
		in = fs.open(new Path(uri));
		try {
			out.writeBytes(fileMetadata);
			IOUtils.copyBytes(in, out, 4096, false);
		} finally {
			IOUtils.closeStream(in);
		}

	}
}
