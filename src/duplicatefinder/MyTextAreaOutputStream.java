/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package duplicatefinder;

import java.io.*;
import javax.swing.*;

public class MyTextAreaOutputStream  extends OutputStream {

  public static final int MAX_SIZE = 10000;
  private JTextArea textArea;

  public MyTextAreaOutputStream(JTextArea textArea) {
    this.textArea = textArea;
  }
  public void write(byte[] b) throws IOException
  {
      checkSize();
      textArea.append(( new String( b ) ));
      setPosition();
  }
  public void write(byte[] b, int off, int len) throws IOException
  {
     checkSize();
     textArea.append( new String( b, off, len ) );
     setPosition();
  }
  public void write(int b) throws IOException
  {
     checkSize();
     textArea.append( ( new Character( (char) b ) ).toString() );
     setPosition();
  }
  public String toString()
  {
    return textArea.getText();
  }

  private void checkSize()
  {
    if(textArea.getText().length()> MAX_SIZE)
       textArea.setText(textArea.getText().substring(MAX_SIZE,textArea.getText().length()));
  }

  private void setPosition()
  {
    textArea.setCaretPosition(textArea.getDocument().getLength());
  }
}