package edu.berkeley.scads.piql;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;

import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.FilenameFilter;
import edu.berkeley.cs.scads.piql.Compiler$;

/**
 * Compile a PIQL Spec to bytecode.
 * @goal compile
 * @phase generate-sources
 */
public class CompilePiqlMojo extends AbstractMojo
{
	/**
	 * The maven project.
	 *
	 * @parameter expression="${project}"
	 * @required
	 * @readonly
	 */
	private MavenProject _project = null;

    private String mkScalaExt(String origName) {
        if (origName.lastIndexOf('.') == -1)
            return origName + ".scala";
        return origName.substring(0, origName.lastIndexOf('.')) + ".scala";
    }

    private static final FilenameFilter hiddenFileFilter = new FilenameFilter() {
        public boolean accept(File dir, String fileName) {
            return !fileName.startsWith(".") && !fileName.startsWith("~");
        }
    };

    private static void recursiveTraversal(File f, List<File> collector) {
        if (f.isDirectory()) {
            for (File child : f.listFiles(hiddenFileFilter)) {
                recursiveTraversal(child, collector);
            }
        } else {
            collector.add(f);
        }
    }

    private static List<File> recursiveTraversal(File f) {
        List<File> ret = new ArrayList<File>();
        recursiveTraversal(f, ret);
        return ret;
    }

	public void execute() throws MojoExecutionException
	{
		File dir = new File(_project.getBasedir(), "src/main/piql");
		File outDir = new File(_project.getBasedir(), "target/generated-sources/piql");
        outDir.mkdirs();
		for (File f : recursiveTraversal(dir)) {
			getLog().info("Compiling:" + f.getName());
            Compiler$.MODULE$.compileToFile(f, new File(outDir, mkScalaExt(f.getName())));
		}
        _project.addCompileSourceRoot(outDir.getAbsolutePath());
	}
}
