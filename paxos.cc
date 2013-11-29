#include "paxos.h"
#include "handle.h"
// #include <signal.h>
#include <stdio.h>
#include "tprintf.h"
#include "lang/verify.h"

// This module implements the proposer and acceptor of the Paxos
// distributed algorithm as described by Lamport's "Paxos Made
// Simple".  To kick off an instance of Paxos, the caller supplies a
// list of nodes, a proposed value, and invokes the proposer.  If the
// majority of the nodes agree on the proposed value after running
// this instance of Paxos, the acceptor invokes the upcall
// paxos_commit to inform higher layers of the agreed value for this
// instance.


bool
operator> (const prop_t &a, const prop_t &b)
{
  return (a.n > b.n || (a.n == b.n && a.m > b.m));
}

bool
operator>= (const prop_t &a, const prop_t &b)
{
  return (a.n > b.n || (a.n == b.n && a.m >= b.m));
}

std::string
print_members(const std::vector<std::string> &nodes)
{
  std::string s;
  s.clear();
  for (unsigned i = 0; i < nodes.size(); i++) {
    s += nodes[i];
    if (i < (nodes.size()-1))
      s += ",";
  }
  return s;
}

bool isamember(std::string m, const std::vector<std::string> &nodes)
{
  for (unsigned i = 0; i < nodes.size(); i++) {
    if (nodes[i] == m) return 1;
  }
  return 0;
}

bool
proposer::isrunning()
{
  bool r;
  ScopedLock ml(&pxs_mutex);
  r = !stable;
  return r;
}

// check if the servers in l2 contains a majority of servers in l1
bool
proposer::majority(const std::vector<std::string> &l1, 
		const std::vector<std::string> &l2)
{
  unsigned n = 0;

  for (unsigned i = 0; i < l1.size(); i++) {
    if (isamember(l1[i], l2))
      n++;
  }
  return n >= (l1.size() >> 1) + 1;
}

proposer::proposer(class paxos_change *_cfg, class acceptor *_acceptor, 
		   std::string _me)
  : cfg(_cfg), acc (_acceptor), me (_me), break1 (false), break2 (false), 
    stable (true)
{
  VERIFY (pthread_mutex_init(&pxs_mutex, NULL) == 0);
  my_n.n = 0;
  my_n.m = me;
}

void
proposer::setn()
{
  my_n.n = acc->get_n_h().n + 1 > my_n.n + 1 ? acc->get_n_h().n + 1 : my_n.n + 1;
}

bool
proposer::run(int instance, std::vector<std::string> cur_nodes, std::string newv)
{
  std::vector<std::string> accepts;
  std::vector<std::string> nodes;
  std::string v;
  bool r = false;

  ScopedLock ml(&pxs_mutex);
  tprintf("start: initiate paxos for %s w. i=%d v=%s stable=%d\n",
	 print_members(cur_nodes).c_str(), instance, newv.c_str(), stable);
  if (!stable) {  // already running proposer?
    tprintf("proposer::run: already running\n");
    return false;
  }
  stable = false;
  setn();
  accepts.clear();
  v.clear();
  if (prepare(instance, accepts, cur_nodes, v)) {

    if (majority(cur_nodes, accepts)) {
      tprintf("paxos::manager: received a majority of prepare responses\n");

      if (v.size() == 0)
	v = newv;

      breakpoint1();

      nodes = accepts;
      accepts.clear();
      accept(instance, accepts, nodes, v);

      if (majority(cur_nodes, accepts)) {
	tprintf("paxos::manager: received a majority of accept responses\n");

	breakpoint2();

	decide(instance, accepts, v);
	r = true;
      } else {
	tprintf("paxos::manager: no majority of accept responses\n");
      }
    } else {
      tprintf("paxos::manager: no majority of prepare responses\n");
    }
  } else {
    tprintf("paxos::manager: prepare is rejected %d\n", stable);
  }
  stable = true;
  return r;
}

// proposer::run() calls prepare to send prepare RPCs to nodes
// and collect responses. if one of those nodes
// replies with an oldinstance, return false.
// otherwise fill in accepts with set of nodes that accepted,
// set v to the v_a with the highest n_a, and return true.
bool
proposer::prepare(unsigned instance, std::vector<std::string> &accepts, 
         std::vector<std::string> nodes,
         std::string &v)
{
  // You fill this in for Lab 6
  // Note: if got an "oldinstance" reply, commit the instance using
  // acc->commit(...), and return false.
  unsigned new_my_n_n = acc->get_n_h().n;
  if ( new_my_n_n > my_n.n)
  {
    my_n.n = new_my_n_n + 1;
  }
  else
  {
    my_n.n = my_n.n + 1;
  }
  my_n.m = me;

  prop_t max_n_a;
  max_n_a.n = 0;
  max_n_a.m = std::string();

  for (unsigned i=0; i<nodes.size(); i++)
  {
    handle h(nodes[i]);
    rpcc *cl = h.safebind();
    if(cl)
    {
      tprintf("proposer::prepare, node[i]:%s\n", nodes[i].c_str());
      paxos_protocol::prepareres res;
      paxos_protocol::preparearg arg;

      arg.instance = instance;
      arg.n = my_n;

      paxos_protocol::status ret = 0;
      ret =cl->call(
          paxos_protocol::preparereq,
          me,
          arg,
          res,
          rpcc::to(1000) 
          );
      if(ret == paxos_protocol::OK)
      {
        if(res.oldinstance)
        {
          tprintf("proposer::prepare, node[i]:%s, oldinstance=True\n",
              nodes[i].c_str());
          acc->commit(instance, res.v_a);
          stable = true;
          return false;
        }
        else if (res.accept)
        {
          accepts.push_back(nodes[i]);
          if(res.v_a.size() != 0 && res.n_a > max_n_a)
          {
            tprintf("proposer::prepare, node[i]:%s, res.accept=True\n",
                nodes[i].c_str());
            v = res.v_a;
            max_n_a = res.n_a;
          }
        }
        else
        {
          tprintf("proposer::prepare, node[i]:%s, Rejected\n",
              nodes[i].c_str());
        }
      }
      else
      {
        tprintf("proposer::prepare, node[i]:%s,"
            "call preparereq FAILED, ret:%d\n",
            nodes[i].c_str(), ret);
      }
    }
    else
    {
      tprintf("proposer::prepare, node[i]:%s\n, FAILED safebind",
          nodes[i].c_str());
    }
  }

  return true;
}

// run() calls this to send out accept RPCs to accepts.
// fill in accepts with list of nodes that accepted.
void
proposer::accept(unsigned instance, std::vector<std::string> &accepts,
        std::vector<std::string> nodes, std::string v)
{
  // You fill this in for Lab 6
  for(unsigned i=0; i<nodes.size(); i++)
  {
    handle h(nodes[i]);
    rpcc *cl = h.safebind();
    if(cl)
    {
      int res = 0;
      paxos_protocol::acceptarg arg;
      arg.n = my_n;
      arg.v = v;
      arg.instance = instance;

      paxos_protocol::status ret = 0;
      ret = cl->call(
          paxos_protocol::acceptreq,
          me,
          arg,
          res,
          rpcc::to(1000)
          );
      if (ret == paxos_protocol::OK)
      {
        if (res)
        {
          accepts.push_back(nodes[i]);
          tprintf("proposer::accept, node[i]:%s, Accepted\n",
              nodes[i].c_str());
        }
        else
        {
          tprintf("proposer::accept, node[i]:%s, Rejected\n",
              nodes[i].c_str());
        }
      }
      else
      {
        tprintf("proposer::prepare, node[i]:%s,"
            "call preparereq FAILED, ret:%d\n",
            nodes[i].c_str(), ret);
      }
    }
    else
    {
      tprintf("proposer::accept, node[i]:%s\n, FAILED safebind",
          nodes[i].c_str());
    }
  }
}

void
proposer::decide(unsigned instance, std::vector<std::string> accepts, 
	      std::string v)
{
  // You fill this in for Lab 6
  for (unsigned i=0; i< accepts.size(); i++)
  {
    handle h(accepts[i]);
    int res = 0;
    paxos_protocol::decidearg arg;
    arg.v = v;
    arg.instance = instance;
    rpcc *cl = h.safebind();
    if (cl)
    {
      cl->call(
          paxos_protocol::decidereq,
          me,
          arg,
          res,
          rpcc::to(1000)
          );
      tprintf("proposer::accept, node[i]:%s\n, called dicidreq, instance:%u,v:%s",
          accepts[i].c_str(), instance, v.c_str());
    }
    else
    {
      tprintf("proposer::accept, node[i]:%s\n, FAILED safebind",
          accepts[i].c_str());
    }
  }
}

acceptor::acceptor(class paxos_change *_cfg, bool _first, std::string _me, 
	     std::string _value)
  : cfg(_cfg), me (_me), instance_h(0)
{
  VERIFY (pthread_mutex_init(&pxs_mutex, NULL) == 0);

  n_h.n = 0;
  n_h.m = me;
  n_a.n = 0;
  n_a.m = me;
  v_a.clear();

  l = new log (this, me);

  if (instance_h == 0 && _first) {
    values[1] = _value;
    l->loginstance(1, _value);
    instance_h = 1;
  }

  pxs = new rpcs(atoi(_me.c_str()));
  pxs->reg(paxos_protocol::preparereq, this, &acceptor::preparereq);
  pxs->reg(paxos_protocol::acceptreq, this, &acceptor::acceptreq);
  pxs->reg(paxos_protocol::decidereq, this, &acceptor::decidereq);
}

paxos_protocol::status
acceptor::preparereq(std::string src, paxos_protocol::preparearg a,
    paxos_protocol::prepareres &r)
{
  // You fill this in for Lab 6
  // Remember to initialize *BOTH* r.accept and r.oldinstance appropriately.
  // Remember to *log* the proposal if the proposal is accepted.
  if (a.instance <= instance_h)
  {
    r.oldinstance = 1;
    r.accept = instance_h;
    r.v_a = values[instance_h];
    tprintf("proposer::preparereq, src:%s, a.instance<=instance_h "
        "a.instance:%u, instance_h:%u, r.v_a:%s\n ",
          src.c_str(), a.instance, instance_h, r.v_a.c_str());
  }
  else if (a.n > n_h)
  {
    n_h = a.n;
    r.oldinstance = 0;
    r.accept = 1;
    r.n_a = n_a;
    r.v_a = v_a;

    l->logprop(n_h);
    tprintf("proposer::preparereq, src:%s, a.instance>instance_h and a.n>a.h "
        "a.instance:%u, instance_h:%u, r.v_a:%s\n",
          src.c_str(), a.instance, instance_h, r.v_a.c_str());
  }
  else
  {
    r.oldinstance = 0;
    r.accept = 0;
    tprintf("proposer::preparereq, src:%s, Else\n",
        src.c_str());
  }
  return paxos_protocol::OK;

}

// the src argument is only for debug purpose
//paxos_protocol::status
//acceptor::acceptreq(std::string src, paxos_protocol::acceptarg a, bool &r)
paxos_protocol::status
acceptor::acceptreq(std::string src, paxos_protocol::acceptarg a, int &r)
{
  // You fill this in for Lab 6
  // Remember to *log* the accept if the proposal is accepted.
  if ( a.instance <= instance_h )
  {
    tprintf("proposer::acceptreq, src:%s, a.instance<instance_h "
        "a.instance:%u, instance_h:%u\n",
          src.c_str(), a.instance, instance_h);
  }
  else if ( a.n >= n_h )
  {
    n_a = a.n;
    v_a = a.v;
    l->logaccept(a.n, a.v);
    r = 1;
    tprintf("proposer::acceptreq, src:%s, a.instance>instance_h and a.n<a.h "
            "a.instance:%u, instance_h:%u, v_a:%s\n",
              src.c_str(), a.instance, instance_h, v_a.c_str());

  }
  else
  {
    r = 0;
    tprintf("proposer::acceptreq, src:%s, Else\n",
        src.c_str());
  }
  return paxos_protocol::OK;
}

// the src argument is only for debug purpose
paxos_protocol::status
acceptor::decidereq(std::string src, paxos_protocol::decidearg a, int &r)
{
  ScopedLock ml(&pxs_mutex);
  tprintf("decidereq for accepted instance %d (my instance %d) v=%s\n", 
	 a.instance, instance_h, v_a.c_str());
  if (a.instance == instance_h + 1) {
    VERIFY(v_a == a.v);
    commit_wo(a.instance, v_a);
  } else if (a.instance <= instance_h) {
    // we are ahead ignore.
  } else {
    // we are behind
    VERIFY(0);
  }
  return paxos_protocol::OK;
}

void
acceptor::commit_wo(unsigned instance, std::string value)
{
  //assume pxs_mutex is held
  tprintf("acceptor::commit: instance=%d has v= %s\n", instance, value.c_str());
  if (instance > instance_h) {
    tprintf("commit: highestaccepteinstance = %d\n", instance);
    values[instance] = value;
    l->loginstance(instance, value);
    instance_h = instance;
    n_h.n = 0;
    n_h.m = me;
    n_a.n = 0;
    n_a.m = me;
    v_a.clear();
    if (cfg) {
      pthread_mutex_unlock(&pxs_mutex);
      cfg->paxos_commit(instance, value);
      pthread_mutex_lock(&pxs_mutex);
    }
  }
}

void
acceptor::commit(unsigned instance, std::string value)
{
  ScopedLock ml(&pxs_mutex);
  commit_wo(instance, value);
}

std::string
acceptor::dump()
{
  return l->dump();
}

void
acceptor::restore(std::string s)
{
  l->restore(s);
  l->logread();
}



// For testing purposes

// Call this from your code between phases prepare and accept of proposer
void
proposer::breakpoint1()
{
  if (break1) {
    tprintf("Dying at breakpoint 1!\n");
    exit(1);
  }
}

// Call this from your code between phases accept and decide of proposer
void
proposer::breakpoint2()
{
  if (break2) {
    tprintf("Dying at breakpoint 2!\n");
    exit(1);
  }
}

void
proposer::breakpoint(int b)
{
  if (b == 3) {
    tprintf("Proposer: breakpoint 1\n");
    break1 = true;
  } else if (b == 4) {
    tprintf("Proposer: breakpoint 2\n");
    break2 = true;
  }
}
