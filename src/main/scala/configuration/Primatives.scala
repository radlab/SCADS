package deploylib.configuration

import deploylib.configuration.ValueConverstion._
import java.io.File

@deprecated
object MachineRoot extends RemoteDirectory(null, new LateBoundValue("MachineRoot", (t: RemoteMachine) => t.rootDirectory.toString))

@deprecated
class RemoteDirectory(parent: RemoteDirectory, name: Value[String]) extends Configuration with Value[String] {
	def this(name: Value[String]) = this(null, name)

	def action(target: RemoteMachine) = target.executeCommand("mkdir -p " + getValue(target))
	def description:String = "Create Directory " + toString

	override def toString: String = {
		if(parent == null)
			name.toString
		else
			parent + "/" + name.toString
	}

	def getValue(target: RemoteMachine):String = {
		if(parent == null)
			name.getValue(target)
		else
			parent.getValue(target) + "/" + name.getValue(target)
	}
}

@deprecated
class RemoteFile(dest: RemoteDirectory, filename: String, contents: Value[String], mode: String) extends Configuration with Value[String]{
	def action(target: RemoteMachine) = {
		val filePath = dest.getValue(target) + "/" + filename
		target.createFile(filePath, contents.getValue(target))
		target.executeCommand("chmod " + mode + " " + filePath)
	}
	def description: String = "Create file " + filename + " at " + dest + " with contents " + contents

	def getValue(target: RemoteMachine) = dest.getValue(target) + "/" + filename
}

@deprecated
class FileUpload(localFile: String, dest: RemoteDirectory) extends Configuration with Value[String] {
	def action(target: RemoteMachine) = {
		target.upload(localFile, dest.getValue(target))
	}
	def description: String = "Upload " + localFile + " to " + dest.toString

	def getValue(target: RemoteMachine) = dest.getValue(target) + "/" + localFile.getName()
}
