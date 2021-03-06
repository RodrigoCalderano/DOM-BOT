#include <Zmq/Zmq.mqh>
#include <Trade\SymbolInfo.mqh>
#include <Trade\Trade.mqh>

extern string PROJECT_NAME = "TradeServer";
extern string ZEROMQ_PROTOCOL = "tcp";
extern string HOSTNAME = "*";
extern int REP_PORT = 5555;
extern int MILLISECOND_TIMER = 1;  // 1 millisecond

extern string t0 = "--- Trading Parameters ---";
extern int MagicNumber = 123456;
extern int MaximumSlippage = 3;


// CREATE ZeroMQ Context
Context context(PROJECT_NAME);

// CREATE ZMQ_REP SOCKET
Socket repSocket(context,ZMQ_REP);

// VARIABLES FOR LATER
uchar myData[];
ZmqMsg request;

CTrade trade;

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
    EventSetMillisecondTimer(MILLISECOND_TIMER);     // Set Millisecond Timer to get client socket input

    Print("[REP] Binding MT4 Server to Socket on Port " + IntegerToString(REP_PORT) + "..");

    repSocket.bind(StringFormat("%s://%s:%d", ZEROMQ_PROTOCOL, HOSTNAME, REP_PORT));

    /*
        Maximum amount of time in milliseconds that the thread will try to send messages 
        after its socket has been closed (the default value of -1 means to linger forever):
    */

    repSocket.setLinger(1000);  // 1000 milliseconds

    /* 
      If we initiate socket.send() without having a corresponding socket draining the queue, 
      we'll eat up memory as the socket just keeps enqueueing messages.
      
      So how many messages do we want ZeroMQ to buffer in RAM before blocking the socket?
    */

    repSocket.setSendHighWaterMark(5);     // 5 messages only.

    return(INIT_SUCCEEDED);
}
  
//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
    Print("[REP] Unbinding MT4 Server from Socket on Port " + IntegerToString(REP_PORT) + "..");
    repSocket.unbind(StringFormat("%s://%s:%d", ZEROMQ_PROTOCOL, HOSTNAME, REP_PORT));
}
//+------------------------------------------------------------------+
//| Expert timer function                                            |
//+------------------------------------------------------------------+
void OnTimer()
{   
    // Get client's response, but don't wait.
    repSocket.recv(request,true);
    
    // MessageHandler() should go here.   
    MessageHandler(request);
}
//+------------------------------------------------------------------+

void MessageHandler(ZmqMsg &localRequest)
{
    // Output object
    ZmqMsg reply;
    
    // Message components for later.
    string components[];
    
    if(localRequest.size() > 0) {
        // Get data from request   
        ArrayResize(myData, localRequest.size());
        localRequest.getData(myData);
        string dataStr = CharArrayToString(myData);
        
        // Process data
        ParseZmqMessage(dataStr, components);
        
        // Interpret data
        InterpretZmqMessage(components);
    }
}

//+------------------------------------------------------------------+
// Interpret Zmq Message and perform actions
void InterpretZmqMessage(string& compArray[])
{
    Print("ZMQ: Interpreting Message..");

    int switch_action = 0;
    string volume;

    if (compArray[0] == "RATES")
        switch_action = 1;
    else if (compArray[0] == "TRADE" && compArray[1] == "BUY")
        switch_action = 2;
    else if (compArray[0] == "TRADE" && compArray[1] == "SELL")
        switch_action = 3;

        
    string ret = "";
    int ticket = -1;
    bool ans = false;

    MqlRates rates[];
    ArraySetAsSeries(rates, true);    

    int price_count = 0;
    
    ZmqMsg msg("[SERVER] Error ocurred on metatrader");
    ret = "N/A";
    switch(switch_action) 
    {
        case 1: 
            if(ArraySize(compArray) > 1) 
                ret = GetCurrent(compArray[1]);
            repSocket.send(ret, false);
            break;
            
        case 2: 
            
             if(trade.Buy(99,compArray[2],100,10,30,""))
               {
                  ret = "Ordem de Compra - sem falha. ResultRetcode: " + trade.ResultRetcode() + ", RetcodeDescription: " + trade.ResultRetcodeDescription();
               }
            else
               {
                  ret = "Ordem de Compra - com falha. ResultRetcode: " + trade.ResultRetcode() + ", RetcodeDescription: " + trade.ResultRetcodeDescription();
               }
            repSocket.send(ret, false);
            break;
            
        case 3:
             if(trade.Sell(100,compArray[2],10,100,5,""))
               {
                  ret = "Ordem de Venda - sem falha. ResultRetcode: " + trade.ResultRetcode() + ", RetcodeDescription: " + trade.ResultRetcodeDescription();
               }
            else
               {
                  ret = "Ordem de Venda - com falha. ResultRetcode: " + trade.ResultRetcode() + ", RetcodeDescription: " + trade.ResultRetcodeDescription();
               }
            
            repSocket.send(ret, false);           
            break;
            
        default: 
            break;
    }
}
//+------------------------------------------------------------------+
// Parse Zmq Message
void ParseZmqMessage(string& message, string& retArray[]) 
{   
    Print("Parsing: " + message);
    
    string sep = "|";
    ushort u_sep = StringGetCharacter(sep,0);
    
    int splits = StringSplit(message, u_sep, retArray);
    
    for(int i = 0; i < splits; i++) {
        Print(IntegerToString(i) + ") " + retArray[i]);
    }
}

//+------------------------------------------------------------------+
string GetCurrent(string symbol)
{
    MqlTick Last_tick;
    double sma20Array[];
    double sma40Array[];
    int sma20Handle;
    int sma40Handle;
    double maximo = SymbolInfoDouble(symbol, SYMBOL_LASTHIGH); 
    double minimo = SymbolInfoDouble(symbol, SYMBOL_LASTLOW);
    SymbolInfoTick(symbol,Last_tick);    
    double bid = Last_tick.bid;
    double ask = Last_tick.ask;
   
    sma20Handle = iMA(symbol, PERIOD_D1, 20, 0, MODE_SMA, PRICE_CLOSE);
    sma40Handle = iMA(symbol, PERIOD_D1, 40, 0, MODE_SMA, PRICE_CLOSE);
    ArraySetAsSeries(sma20Array, true);
    ArraySetAsSeries(sma40Array, true);
    CopyBuffer(sma20Handle, 0, 0, 3, sma20Array);
    CopyBuffer(sma40Handle, 0, 0, 3, sma40Array);

    MarketBookAdd(symbol);
    return(StringFormat("%.2f,%.2f,%.2f,%.2f,%.2f,%.2f", bid, ask, maximo, minimo, sma20Array[0], sma40Array[0]));
}

//+------------------------------------------------------------------+