package edu.berkeley.scads.piql;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;

import java.io.File;
import edu.berkeley.cs.scads.Compiler$;

/**
 * Compile a PIQL Spec to bytecode.
 * @goal compile
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

	public void execute() throws MojoExecutionException
	{
		File dir = new File("src/main/piql");
		File outDir = new File("target/classes");
		StringBuilder cp = new StringBuilder();

		try {
			for(Object elm : _project.getRuntimeClasspathElements()) {
				cp.append((String)elm);
				cp.append(":");
			}
		}
		catch(Exception e)
		{
			getLog().error(e.getMessage(),e);
			throw new MojoExecutionException(e.getMessage());
		}

		System.setProperty("surefire.test.class.path", cp.toString());

		for(String filename : dir.list()) {
			getLog().info("Compiling:" + filename);
			getLog().info("classpath" + cp.toString());
			String code = Compiler$.MODULE$.codeGenFromFile(dir + "/" + filename);
			Compiler$.MODULE$.compileSpecCode(outDir, cp.toString(), code);
		}
	}
}

