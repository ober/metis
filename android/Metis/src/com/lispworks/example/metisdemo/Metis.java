/*
 ** $Header: /hope/lwhope1-cam/hope.0/compound/23/LISPexamples/RCS/android:MetisDemo:src:com:lispworks:example:metisdemo:Metis.java,v 1.12.1.1 2014/05/27 20:55:54 davef Exp $
 **
 ** Copyright (c) 1987--2015 LispWorks Ltd. All rights reserved.
 */
package com.lispworks.example.metisdemo;

// A board for playing Metis.
// This just displays the board and interact with the user. The
// actual playing, i.e. deciding what effect touching a square has, 
// which squares to draw in what colour, and (if the computer plays) decide 
///on computer moves, is done by an 
// "Metis Server", which needs to implement an MetisServer interface
// to get called, and call some methods in this class. The server
// is then called whenever the user touch the board, and needs to decide
// how to update the board.
// The code here also contains an implementation of the interface which
// just calls directly into Lisp, using the functions that are defined in 
// (example-file "android/android-metis-user"). 

// Because the board just displays and reports touches to the server,
// the actual game that is played is defined by the server, rather than by the board.
// Any game which can be played by single touches and needs only two colour
// "pieces" for display can be implemented by the server

import android.app.Activity;
import android.os.Bundle;
import android.os.Vibrator;
import android.widget.*;
import android.content.*;
import android.view.*;
import android.util.*;

public class Metis extends Activity {
	static final int WHITE = 1; // these two need to match the definitions in
								// (example-file "android/android-metis-user")
	static final int BLACK = 2;
	final static int METIS_SERVER_TYPE_JAVA = 0;
	final static int METIS_SERVER_TYPE_FULL_PROXY = 1; // These two need to match what CREATE-LISP-METIS-SERVER
	final static int METIS_SERVER_TYPE_LAZY_PROXY = 2; // in (example-file "android/android-metis-user") expects

	static int mServerType = METIS_SERVER_TYPE_JAVA;

	static LinearLayout mMetisGrid; // On API 14 that can be a GridLayout, but
										// we want to support API 10
	static MetisServer mMetisServer = null;
	static TextView mTextView;
	static Vibrator mVibrator;
	static SubMenu mServerMenu = null;

	// Interface

	// Methods that the "server" should call
	public static void updateState(String string) {
		mTextView.setText(string);
	}

	public static void signalBadMove() {
		mVibrator.vibrate(50);
	}

	public static void change(int index, int what) {
		ImageView iv = (ImageView) (((LinearLayout) mMetisGrid
				.getChildAt(index >> 3)).getChildAt(index & 7));
		int id = what_to_id(what);
		iv.setImageResource(id);

	}

	// Interface that the "server" needs to implement.
	// We have a default implemenation below that calls into lisp
	public interface MetisServer {
		void init(boolean reset);

		void playSquare(int square);

		int undoMove();

		void setComputerPlays(boolean on);
	}

	// End of public interface

	// A default implementation of the MetisServer. Assumes that Lisp is
	// loaded  and got the functions defined (example-file
	// "android/android-metis-user")
	class JavaMetisServer implements MetisServer {
		public void init(boolean reset) {
			com.lispworks.LispCalls.callVoidV("init-metis", reset);
		}

		public void playSquare(int square) {
			com.lispworks.LispCalls.callVoidV("metis-clicked", square);
		}

		public int undoMove() {
			return com.lispworks.LispCalls.callIntV("undo-move");
		}

		public void setComputerPlays(boolean on) {
			com.lispworks.LispCalls.callIntV("set-computer-plays", on);
		}
	}

	// Use this when lisp goes wrong, to ensure that we don't try to call into
	class ErrorMetisServer implements MetisServer {
		public void init(boolean reset) {
			showLispPanel();
		}

		public void playSquare(int square) {
			showLispPanel();
		}

		public int undoMove() {
			showLispPanel();
			return 0;
		}

		public void setComputerPlays(boolean on) {
			showLispPanel();
		}
	}

	// / The listener object, one per square. We need to remember
	// the index to pass it to the server when clicked.
	static class SquareListener implements View.OnClickListener {
		int mIndex;

		public SquareListener(int index) {
			mIndex = index;
		}

		public void onClick(View view) {
			mMetisServer.playSquare(mIndex);
		}
	}

	// Input is an integer from the server, we map to an id of drawable,
	// which is the image that we draw.
	static int what_to_id(int what) {
		return (what == 0) ? R.drawable.empty
				: ((what == WHITE) ? R.drawable.white : R.drawable.black);
	}

	static void resetAllSquares() {
		for (int index = 0; index < 64; index++) {
			change(index, 0);
		}
	}

	// / buttons
	// Game control, we just invoke the server method
	public void computerPlays(View view) {
		CheckBox cb = (CheckBox) view;
		boolean checked = cb.isChecked();
		mMetisServer.setComputerPlays(checked);
	}

	
	// end of button methods

	// Options menu support

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		MenuInflater inflater = getMenuInflater();
		boolean with_lisp = LispPanel.canEvaluate();
		int id = with_lisp ? R.menu.metis_menu : R.menu.metis_menu_no_lisp;
		inflater.inflate(id, menu);

		if (with_lisp) {
			MenuItem mi = menu.findItem(R.id.menu_serveritem);
			mServerMenu = mi.getSubMenu();

                       	// set proxy item to indicate the right server
			int itemId = R.id.menu_javaserver;
			if (mServerType == METIS_SERVER_TYPE_FULL_PROXY)
				itemId = R.id.menu_proxyfull;
			else if (mServerType == METIS_SERVER_TYPE_LAZY_PROXY)
				itemId = R.id.menu_proxylazy;
			MenuItem serverMi = mServerMenu.findItem(itemId);
			serverMi.setChecked(true);

		}
		return true;
	}

	@Override
	public boolean onOptionsItemSelected(MenuItem item) {
		// Handle item selection
		switch (item.getItemId()) {
		case R.id.menu_restart:
			resetAllSquares(); // the server expects clean board when "init" is
								// called
			mMetisServer.init(true); // true means restart the game.
			return true;
		case R.id.menu_undo:
			mMetisServer.undoMove();
			return true;
		case R.id.menu_lisp_panel:
		case R.id.menu_output:
			showLispPanel();
			return true;
		case R.id.menu_history:
			startActivityForResult(LispPanel.createIntentForHistory(), 1);
			return true;
		case R.id.menu_proxyfull:
			setupServer(METIS_SERVER_TYPE_FULL_PROXY);
			return true;
		case R.id.menu_proxylazy:
			setupServer(METIS_SERVER_TYPE_LAZY_PROXY);
			return true;
		case R.id.menu_javaserver:
			setupServer(METIS_SERVER_TYPE_JAVA);
			return true;

		default:
			return super.onOptionsItemSelected(item);
		}
	}

	// end of options menu support
	
	// Internal bits

	// setupServer - sets the server that decides what happens on the Metis
	// board to an
	// an interface MetisServer. The interface can be either a Java class
	// LispMetisServer which is defined above
	// to implement the methods by direct calls to lisp, or a lisp proxy which
	// is defined in Lisp.
	// The proxies in lisp are either "full", defining a function for each
	// method of the interface,
	// or "lazy", which defines only the default function which is called for
	// all the methods.
	// The lisp function and the proxies are defined in (example-file
	// "android/android-metis-user")
	void setupServer(int which) {
		if (com.lispworks.Manager.status() != com.lispworks.Manager.STATUS_READY) {
			mMetisServer = new ErrorMetisServer();
			return;
		}
		mServerType = which; // remember it
		int id = R.id.menu_javaserver;
		switch (which) {
		case METIS_SERVER_TYPE_JAVA:
			mMetisServer = new JavaMetisServer();
			break; // / Use the Java proxy defined above

		// demonstrate creating the proxy by calling a lisp function that
		// creates it by using make-lisp-proxy
		case METIS_SERVER_TYPE_FULL_PROXY: {
			Object obj = com.lispworks.LispCalls.callObjectV(
					"CREATE-LISP-METIS-SERVER", which);
			if (obj != null) // In case of error, Lisp should have already
								// reported it.
				mMetisServer = (MetisServer) obj;
			id = R.id.menu_proxyfull;
		}
			;
			break;

		// demonstrate creating the proxy by passing its name to createLispProxy
		case METIS_SERVER_TYPE_LAZY_PROXY: {
			Object obj = com.lispworks.LispCalls
					.createLispProxy("LISP-METIS-SERVER-LAZY");
			if (obj != null) // In case of error, Lisp should have already
								// reported it.
				mMetisServer = (MetisServer) obj;
			id = R.id.menu_proxylazy;
		}
			;
			break;
		}
		if (mServerMenu != null) { // can be called before there is options menu
			MenuItem mi = mServerMenu.findItem(id);
			mi.setChecked(true);
		}

	}

	protected void onCreate(Bundle savedInstanceState) {

		super.onCreate(savedInstanceState);
		mMetisServer = new ErrorMetisServer(); // in case it goes wrong Ensure we have a server.
		Intent in = getIntent();
		Bundle extras = in.getExtras();
		if (extras != null)
			mServerType = extras.getInt("MetisServerType", mServerType);

		Context co = getApplicationContext();

		// set the layout and initial some statics.
		setContentView(R.layout.metis);

		SquareLayout sl = (SquareLayout) findViewById(R.id.metis_grid);
		mMetisGrid = new LinearLayout(co);
		mMetisGrid.setOrientation(LinearLayout.VERTICAL);
		sl.addView(mMetisGrid);
	
		mTextView = (TextView) findViewById(R.id.MetisState);
		mVibrator = (Vibrator) getSystemService(VIBRATOR_SERVICE);

		// Fill the the grid with 64 ImageView objects corresponding to the
		// 64 squares. Each ImageView has a listener which remembers its index,
		// and can pass it to the server.
		LinearLayout currentLine = null;
		LinearLayout.LayoutParams vertical_stretch_lp = new LinearLayout.LayoutParams(
				LinearLayout.LayoutParams.MATCH_PARENT, 0, (float) 1);
		LinearLayout.LayoutParams horizontal_stretch_lp = new LinearLayout.LayoutParams(
				0, LinearLayout.LayoutParams.MATCH_PARENT, (float) 1);

		for (int index = 0; index < 64; index++) {
			if (0 == (index & 7)) {
				currentLine = new LinearLayout(co);
				mMetisGrid.addView(currentLine, vertical_stretch_lp);
			}
			ImageView iv = new ImageView(co);
			SquareListener li = new SquareListener(index);
			iv.setImageResource(R.drawable.empty);
			iv.setOnClickListener(li);
			iv.setMinimumWidth(1);
			iv.setMinimumHeight(1);

			iv.setScaleType(android.widget.ImageView.ScaleType.FIT_CENTER);
			currentLine.addView(iv, horizontal_stretch_lp);
		}

		setupAndInit();
	}

        // setupAndInit  -  Ensure that Lisp is working, and when it is
        // initialize the game. 
	void setupAndInit() {
		int lwStatus = com.lispworks.Manager.status();
		switch (lwStatus) {
		case com.lispworks.Manager.STATUS_READY:
			setupServer(mServerType);
			// tell the server to get going. Passing false tells
			// the server to continue existing game if there is any.
			mMetisServer.init(false);

			break;
		case com.lispworks.Manager.STATUS_ERROR: // give up
			mMetisServer = new ErrorMetisServer();

			com.lispworks.Manager.addMessage("Failed to initialize LispWorks("
					+ com.lispworks.Manager.init_result_code() + ") : "
					+ com.lispworks.Manager.mInitErrorString,
					com.lispworks.Manager.ADDMESSAGE_RESET);
			showLispPanel();

			break;
		case com.lispworks.Manager.STATUS_INITIALIZING:
		case com.lispworks.Manager.STATUS_NOT_INITIALIZED:
			// Initialize and run again this method.
			Runnable rn = new Runnable() {
				public void run() {
					setupAndInit();
				}
			};

			com.lispworks.Manager.init(this, rn);
			break;
		}

	}

	// Switch to "Lisp Panel" of the application.
	void showLispPanel() {
		startActivity(LispPanel.createIntent("LispPanel"));
	}

	// Get invoked as result of a call to startActivityForResult.
	protected void onActivityResult(int requestCode, int resultCode,
			android.content.Intent data) {
		switch (requestCode) {
		case 1: // showHistory above.
			if (resultCode == RESULT_OK) {
				// Got a useful result, stick it in the input pane and switch
				// the "Lisp Panel"
				String str = data.getStringExtra("result");
				LispPanel.setInputText(str);
				showLispPanel();
				break;
			}
		}
	}

}
