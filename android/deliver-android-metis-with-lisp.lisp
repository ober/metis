;; -*- Mode: Lisp; rcs-header: "$Header: /hope/lwhope1-cam/hope.0/compound/23/LISPexamples/RCS/android:deliver-android-metis-with-lisp.lisp,v 1.4.1.1 2014/05/27 20:55:56 davef Exp $" -*-

;;----------------------------------------------------------------------------
;;
;; examples/capi/andrioid/deliver-android-metis-with-lisp.lisp
;;
;; A delivery file for the Android Metis example, delivering "with 
;; Lisp", which means keeping enough that most forms can be evaluated.
;; For that we deliver in level 4 rather than 5, keep macros
;; so things like DEFUN work, and keep the externals in the
;; interesting packages. We also keep the symbol EVAL-FOR-ANDROID,
;; which is defined in (example-file "android/android-metis-user.lisp"),
;; and its existence is used by the Java side to decide if it 
;; can evaluate forms. 
;; 
;; Copy this file and edit the value of *PROJECT-PATH*
;; to point to where you have your copy of the MetisDemo
;; project. See README for details. 
;;
;; Then use this file on an ARM machine as the build script for the
;; Android deliveryimage: 
;;
;; <Android-delivery-image> -init <copy-of-this-file>
;;
;; The android-delivery-image in LispWorks 7.0 is called 
;; lispworks-7-0-0-arm-linux-android.
;;
;; To deliver "without Lisp", use deliver-android-metis.lisp instead,
;; which generates a much smaller application.  
;;----------------------------------------------------------------------------
;; Copyright (c) 1987--2015 LispWorks Ltd. All rights reserved.
;;----------------------------------------------------------------------------


(defvar *project-path* "/home/jaimef/StudioProjects/MetisDemo"
  "Points to the directory where the MetisDemo project is")


;;; The next two lines "load the application".
;;; We use :load :delete :output-file :temp to avoid compiling in the examples directory, 
;;; and not leaving the compiled file in the directory. 
(load "~/quicklisp/setup.lisp")
(compile-file (example-file "misc/metis") :load :delete :output-file :temp)
(compile-file (example-file "android/android-metis-user.lisp") :load :delete :output-file :temp)

;;; Also load some examples
(compile-file (example-file "android/dialog") :load :delete :output-file :temp)
(compile-file (example-file "android/dialog") :load :delete :output-file :temp)
(compile-file "~/quicklisp/setup.lisp" :load :delete :output-file :temp)
(ql:quickload '(:fare-memoization :cl-fad :gzip-stream :cl-json :s-sql :pcall :uiop :cl-store :postmodern))

(load "~/metis/package.lisp")

(require 'asdf)
(asdf:operate 'asdf:load-op 'metis)


;;; Needs this line because there is no Lisp code that refers to these symbols,
;;; they are passed to read and eval from Java.

(deliver-keep-symbols 'raise-alert-dialog 'raise-a-toast 'metis:main)

(deliver-to-android-project nil *project-path* 0 :keep-package-manipulation t :multiprocessing t :keep-eval t :keep-fasl-dump t :keep-editor t :keep-foreign-symbols t :keep-function-name t :keep-gc-cursor t :keep-keyword-names t :keep-lisp-reader t :keep-macros t :keep-modules t :keep-top-level t :license-info nil :keep-walker t :KEEP-PRETTY-PRINTER t)

(quit)
