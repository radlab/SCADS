package edu.berkeley.scads.piql;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;

import java.io.File;
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

	public void execute() throws MojoExecutionException
	{
		File dir = new File(_project.getBasedir(), "src/main/piql");
		File outDir = new File(_project.getBasedir(), "target/generated-sources/piql");
        outDir.mkdirs();
		for(String filename : dir.list()) {
			getLog().info("Compiling:" + filename);
            Compiler$.MODULE$.compileToFile(new File(dir + "/" + filename), new File(outDir + "/" + mkScalaExt(filename)));
		}
        _project.addCompileSourceRoot(outDir.getAbsolutePath());
	}
}
