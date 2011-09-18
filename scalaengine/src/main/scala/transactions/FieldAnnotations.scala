package edu.berkeley.cs.scads.storage

import annotation.target.field

object FieldAnnotations {
  type FieldGT = JavaFieldAnnotations.JavaFieldGT @field
  type FieldGE = JavaFieldAnnotations.JavaFieldGE @field
  type FieldLT = JavaFieldAnnotations.JavaFieldLT @field
  type FieldLE = JavaFieldAnnotations.JavaFieldLE @field
}
