package edu.berkeley.cs.scads.mojo;

import java.io.File;
import java.io.IOException;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;
import org.apache.maven.shared.model.fileset.FileSet;
import org.apache.maven.shared.model.fileset.util.FileSetManager;

import edu.berkeley.cs.scads.avro.compiler.CompilerMain$;

/**
 * Compile an Avro protocol schema file.
 *
 * @goal protocol
 * @phase generate-sources
 */
public class ProtocolMojo extends AbstractMojo {
    /**
     * @parameter expression="${sourceDirectory}" default-value="${basedir}/src/main/avro"
     */
    private File sourceDirectory;

    /**
     * @parameter expression="${outputDirectory}" default-value="${project.build.directory}/generated-sources/avro"
     */
    private File outputDirectory;

    /**
     * A set of Ant-like inclusion patterns used to select files from
     * the source directory for processing. By default, the pattern
     * <code>**&#47;*.avro</code> is used to select grammar files.
     *
     * @parameter
     */
    private String[] includes = new String[] { "**/*.avpr" };

    /**
     * A set of Ant-like exclusion patterns used to prevent certain
     * files from being processed. By default, this set is empty such
     * that no files are excluded.
     *
     * @parameter
     */
    private String[] excludes = new String[0];

    /**
     * The current Maven project.
     *
     * @parameter default-value="${project}"
     * @readonly
     * @required
     */
    private MavenProject project;

    private FileSetManager fileSetManager = new FileSetManager();

    public void execute() throws MojoExecutionException {
        if (!sourceDirectory.isDirectory()) {
            throw new MojoExecutionException(sourceDirectory
                    + "is not a directory");
        }

        FileSet fs = new FileSet();
        fs.setDirectory(sourceDirectory.getAbsolutePath());
        fs.setFollowSymlinks( false );

        for (String include : includes) {
            fs.addInclude(include);
        }
        for (String exclude : excludes) {
            fs.addExclude(exclude);
        }

        outputDirectory.mkdirs();

        String[] includedFiles = fileSetManager.getIncludedFiles(fs);

        for (String filename : includedFiles) {
            try {
                CompilerMain$.MODULE$.main(new String[] {
                        new File(sourceDirectory, filename).getAbsolutePath(),
                        new File(outputDirectory, rename(filename)).getAbsolutePath()
                        } );
            } catch (Exception e) {
                throw new MojoExecutionException("Error compiling protocol file "
                        + filename + " to " + outputDirectory, e);
            }
        }

        project.addCompileSourceRoot(outputDirectory.getAbsolutePath());
    }

    private String rename(String name) {
        int lastIdx = name.lastIndexOf('.');
        if (lastIdx == -1)
            return name + ".scala";
        return name.substring(0, lastIdx) + ".scala";
    }
}
