(require 'asdf)
(require 'cmp)

(setf *load-verbose* nil)
(setf *compile-verbose* nil)
(setf c::*suppress-compiler-warnings* t)
(setf c::*suppress-compiler-notes* t)
(setf c::*compile-in-constants* t)

(push (make-pathname :name nil :type nil :version nil :defaults *load-truename*)
      asdf:*central-registry*)

(ql:quickload :metis)

;;(asdf:operate 'asdf:load-op 'metis)

;;(asdf:operate 'asdf:load-bundle-op 'uiop)
;;(asdf:operate 'asdf:load-bundle-op 'metis)


;;(asdf:make-build :metis :type :program :epilogue-code 'metis::main :move-here "./")


(asdf:operate 'asdf:program-op :metis)
