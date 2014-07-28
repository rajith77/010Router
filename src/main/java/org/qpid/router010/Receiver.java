package org.qpid.router010;

import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.messenger.Messenger;

public class Receiver
{

    public static void main(String[] args)
    {
        Messenger mng = Proton.messenger();
        try
        {
            mng.start();
            mng.subscribe("localhost:5672/on_weather");
            while (true)
            {
                mng.recv();
                while (mng.incoming() > 0)
                {
                    Message msg = mng.get();
                    System.out.println("Address : " + msg.getAddress());
                    System.out.println("Subject : " + msg.getSubject());
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            mng.stop();
        }
    }

}
