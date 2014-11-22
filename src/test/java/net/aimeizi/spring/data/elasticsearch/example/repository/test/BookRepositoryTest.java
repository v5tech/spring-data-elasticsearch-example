package net.aimeizi.spring.data.elasticsearch.example.repository.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.aimeizi.spring.data.elasticsearch.example.Author;
import net.aimeizi.spring.data.elasticsearch.example.Book;
import net.aimeizi.spring.data.elasticsearch.example.repository.BookRepository;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.SuggestBuilder.SuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.FacetedPage;
import org.springframework.data.elasticsearch.core.FacetedPageImpl;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * 详情参考:https://github.com/BioMedCentralLtd/spring-data-elasticsearch-sample-application
 * @author Administrator
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class BookRepositoryTest{

	@Autowired
	private BookRepository repository;

	@Autowired
	private ElasticsearchTemplate elasticsearchTemplate;
	
	@Before
	public void before() {
		elasticsearchTemplate.deleteIndex(Book.class);
		elasticsearchTemplate.createIndex(Book.class);
		initIndex();
		elasticsearchTemplate.refresh(Book.class, true);
	}
	
	/**
	 * 初始化索引
	 */
	public void initIndex(){
		Book book1 = new Book();
		book1.setId(1);
		book1.setName("云去云来");
		book1.setDesc("水深水浅，云去云来，听林青霞的第一本有声书。林青霞说：宋代词人蒋捷的《听雨》，这何尝不是我内心的写照。“少年听雨歌楼上，红烛昏罗帐”，那些年在台湾拍戏拍得火红火绿的。“壮年听雨客舟中，江阔云低，断雁叫西风”，而立之年，孤身在香港拍戏，一待就是十年，曾经试过，独自守着窗儿，对着美丽绚烂的夜景，寂寞得哭泣。“而今听雨僧庐下，鬓已星星也。悲欢离合总无情，一任阶前，点滴到天明。”而今真是鬓已星星也，到了耳顺之年，历尽人生的甜酸苦辣、生离死别，接受了这些人生必经的过程，心境渐能平和，如今能够看本好书，与朋友交换写作心得，已然满足。人生很难有两个甲子，我唯一一个甲子的岁月出了第二本书《云去云来》，当是给自己的一份礼物，也好跟大家分享我这一甲子的人、事、情");
		book1.setAuthor(new Author("1","林青霞"));
		book1.setUrl("http://book.douban.com/subject/26133987/");
		book1.setPubinfo("广西师范大学出版社");
		book1.setPubdate("2014-11");
		book1.setPrice(79.00);
		
		
		Book book2 = new Book();
		book2.setId(2);
		book2.setName("这些人，那些事");
		book2.setDesc("吴念真累积多年、珍藏心底的体会与感动。他写的每个故事，都蕴藏了我们无法预知的生命能量与心灵启发。跟他一起回望人生种种，您将学会包容、豁达与感恩……本书是吴念真导演经历过人生的风风雨雨和最大低潮后，所完成的生命记事。他用文字写下心底最挂念的家人、日夜惦记的家乡、一辈子搏真情的朋友，以及台湾各个角落里最真实的感动。这些人和事，透过他真情挚意的笔，如此跃然的活在你我眼前，笑泪交织的同时，也无可取代的成为烙印在你我心底、这一个时代的美好缩影……特别收录 吴念真近年唯一小说创作《遗书》，写下对胞弟离开人间的真情告白特别邀请 作家雷骧绘制插画，看两位大师以图文激荡出精采火花生命里某些当时充满怨怼的曲折，在后来好像都成了一种能量和养分……这些人、那些事在经过时间的筛滤之后，几乎都只剩下笑与泪与感动和温暖。——《这些人，那些事》");
		book2.setAuthor(new Author("2","吴念真"));
		book2.setUrl("http://book.douban.com/subject/6388661/");
		book2.setPubinfo("译林出版社");
		book2.setPubdate("2011-9");
		book2.setPrice(28.00);
		
		Book book3 = new Book();
		book3.setId(3);
		book3.setName("我们为什么会分手");
		book3.setDesc("“我们似乎热恋过，但我们似乎从未彼此了解。”任何一次分手，留下的都不该只是伤痛，或是对爱情的怀疑，也可以是一次成长。遭遇过分手的人都会问：“我们为什么会分手？”而恰恰正是经历同一段感情的 两个人，会给出不同的答案。分手是如此微妙，让作者毛路和赵珈禾萌生了采访分手情侣的想法。她们在网络上征集，愿意单方面讲述的人众多，双方都愿意的则寥寥无几。在三年多的坚持下，最终采访到22对分手恋人。本书会与您分享其中15对的故事。在男女双方分别回顾恋爱过程、讲述各自心中真实的分手理由时，你会惊讶地发现，性别视角和思维方式上的差异，竟让同一段");
		book3.setAuthor(new Author("3","毛路"));
		book3.setUrl("http://book.douban.com/subject/26146992/");
		book3.setPubinfo("北京联合出版公司");
		book3.setPubdate("2014-11-1");
		book3.setPrice(32.00);
		
		Book book4 = new Book();
		book4.setId(4);
		book4.setName("挪威的森林");
		book4.setDesc("这是一部动人心弦的、平缓舒雅的、略带感伤的恋爱小说。小说主人公渡边以第一人称展开他同两个女孩间的爱情纠葛。渡边的第一个恋人直子原是他高中要好同学木月的女友，后来木月自杀了。一年后渡边同直子不期而遇并开始交往。此时的直子已变得娴静腼腆，美丽晶莹的眸子里不时掠过一丝难以捕捉的阴翳。两人只是日复一日地在落叶飘零的东京街头漫无目标地或前或后或并肩行走不止。直子20岁生日的晚上两人发生了性关系，不料第二天直子便不知去向。几个月后直子来信说她住进一家远在深山里的精神疗养院。渡边前去探望时发现直子开始带有成熟女性的丰腴与娇美。晚间两人虽同处一室，但渡边约束了自己，分手前表示永远等待直子。返校不久，由于一次偶然相遇，渡边开始与低年级的绿子交往。绿子同内向的直子截然相反，“简直就像迎着春天的晨光蹦跳到世界上来的一头小鹿”。这期间，渡边内心十分苦闷彷徨。一方面念念不忘直");
		book4.setAuthor(new Author("4","村上春树 "));
		book4.setUrl("http://book.douban.com/subject/2159042/");
		book4.setPubinfo("上海译文出版社");
		book4.setPubdate("2007-7");
		book4.setPrice(23.00);
		
		Book book5 = new Book();
		book5.setId(5);
		book5.setName("盗墓笔记2");
		book5.setDesc("朋友老痒出狱，给刚从西礁海底墓归来、在家赋闲没有几日的主人公——“我”带来一个惊人的消息：诡异的六角铃铛，古老的厍族，巨大的青铜树，遥远的秦岭腹地……“我”不由得跃跃欲试。接下来，“我”和老痒二人孤身深入到神秘莫测的秦岭探险。但前方等待着他们的又是什么？——各种诡异事物接踵而来，哲罗鲑，黄泉瀑布，尸阵，麒麟竭，烛九阴……这棵巨大的青铜树究竟是做什么用的？是一棵许愿树，还是一个少数民族的图腾？他们到底能不能找到真正的答案？探险的过程充满了人性的挣扎和努力，可怖的人物与可憎的面孔交织出现。最后，是一个让人瞠目结舌，超乎所有想象都无法猜透，却又似乎真实可信的结局");
		book5.setAuthor(new Author("5","南派三叔"));
		book5.setUrl("http://book.douban.com/subject/2057285/");
		book5.setPubinfo("中国友谊出版公司");
		book5.setPubdate("2007-4");
		book5.setPrice(26.80);
		
		repository.save(Arrays.asList(book1, book2, book3, book4, book5));
	}
	
	@Test
	public void doBulkIndexDocument() {
		
		Book book1 = repository.findOne(1);
		print(book1);
		Book book2 = repository.findOne(2);
		print(book2);
		Book book3 = repository.findOne(3);
		print(book3);
		Book book4 = repository.findOne(4);
		print(book4);
		Book book5 = repository.findOne(5);
		print(book5);
		
	}

	@Test
	public void findByNameAndPrice(){
		initIndex();
		List<Book> list = repository.findByNameAndPrice("挪威的森林", 23.00f);
		for (Book book : list) {
			print(book);
		}
	}
	
	@Test
	public void findByPrice(){
		initIndex();
		Page<Book> page = repository.findByPrice(23.00f, new PageRequest(0, 20));
		for (Book book : page.getContent()) {
			print(book);
		}
	}
	
	@Test
	public void deleteBook(){
		initIndex();
		repository.delete(2);
		Book book = repository.findOne(2);
		print(book);
	}
	
	@Test
	public void findAllBooks(){
		initIndex();
		Iterable<Book> all = repository.findAll();
		Iterator<Book> iterator = all.iterator();
		while (iterator.hasNext()) {
			Book book = (Book) iterator.next();
			print(book);
		}
	}
	
	
	@Test
	public void deleteAllBooks(){
		initIndex();
		repository.deleteAll();
		Iterable<Book> all = repository.findAll();
		Iterator<Book> iterator = all.iterator();
		while (iterator.hasNext()) {
			Book book = (Book) iterator.next();
			print(book);
		}
		elasticsearchTemplate.deleteIndex(Book.class);//删除索引目录
	}
	
	
	@Test
	public void sortByPrice(){
		initIndex();
		Iterable<Book> all = repository.findAll(new Sort(new Sort.Order(Sort.Direction.DESC,"price")));
		Iterator<Book> iterator = all.iterator();
		while (iterator.hasNext()) {
			Book book = (Book) iterator.next();
			print(book);
		}
		elasticsearchTemplate.deleteIndex(Book.class);//删除索引目录
	}
	
	@Test
	public void queryTearm(){
		initIndex();
		TermQueryBuilder termQuery = QueryBuilders.termQuery("desc", "内心");
		Iterator<Book> iterator = repository.search(termQuery).iterator();
		while (iterator.hasNext()) {
			Book book = (Book) iterator.next();
			print(book);
		}
		elasticsearchTemplate.deleteIndex(Book.class);//删除索引目录
	}
	
	
	@Test
	public void count(){
		initIndex();
		long count = repository.count();
		System.out.println(count);
	}
	
	
	@Test
	public void isExists(){
		initIndex();
		boolean exists = repository.exists(2);
		System.out.println(exists);
	}
	
	/**
	 * 高亮查询
	 */
	@Test
	public void searchQuery(){
		initIndex();
		QueryBuilder queryBuilder = QueryBuilders.queryString("内心");
		SearchQuery searchQuery = new NativeSearchQueryBuilder()
		.withQuery(queryBuilder)
		.withHighlightFields(new HighlightBuilder.Field("name").preTags("<em>").postTags("</em>").fragmentSize(250))
		.withHighlightFields(new HighlightBuilder.Field("desc").preTags("<em>").postTags("</em>").fragmentSize(250))
		.withPageable(new PageRequest(0, 20))
		.build();
		Page<Book> queryForPage = elasticsearchTemplate.queryForPage(searchQuery, Book.class, new SearchResultMapper() {
			
			@SuppressWarnings("unchecked")
			@Override
			public <T> FacetedPage<T> mapResults(SearchResponse response,
					Class<T> clazz, Pageable pageable) {
				System.out.println("本次查询共耗时:"+response.getTookInMillis()+"毫秒.");
				List<Book> books = new ArrayList<Book>();
				for(SearchHit searchHit : response.getHits()){
					if (response.getHits().getHits().length <= 0) {
						return null;
					}
					Book book = new Book();
					Map<String, Object> source = searchHit.getSource();
					book.setId((Integer)source.get("id"));
					book.setPrice((Double)source.get("price"));
					Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
					HighlightField highlightNameField = highlightFields.get("name");
					if(highlightNameField!=null&&highlightNameField.fragments()!=null){
						book.setName(highlightNameField.fragments()[0].string());
					}else{
						book.setName((String)source.get("name"));
					}
					HighlightField highlightDescField = highlightFields.get("desc");
					if(highlightDescField!=null&&highlightDescField.fragments()!=null){
						book.setDesc(highlightDescField.fragments()[0].string());
					}else{
						book.setDesc((String)source.get("desc"));
					}
					Map<String,String> map = (Map<String, String>) source.get("author");
					Author author = new Author();
					author.setId((String)map.get("id"));
					author.setName((String)map.get("name"));
					book.setAuthor(author);
					book.setUrl((String)source.get("url"));
					book.setPubinfo((String)source.get("pubinfo"));
					book.setPubdate((String)source.get("pubdate"));
					books.add(book);
				}
				if (books.size() > 0) {
					return new FacetedPageImpl<T>((List<T>) books);
				}
				return null;
			}
			
		});
		System.out.println("总页数:"+queryForPage.getTotalPages());
		System.out.println("总记录数:"+queryForPage.getTotalElements());
		List<Book> content = queryForPage.getContent();
		for (Book book : content) {
			print(book);
		}
		elasticsearchTemplate.deleteIndex(Book.class);//删除索引目录
	}
	
	@Test
	public void suggest(){
//		SuggestionBuilder<?> suggestionBuilder = new TermSuggestionBuilder("mySuggestion");
//		suggestionBuilder.text("内心 消息 民族 性虐 优乐美 聚划算 德芙 加多宝 好声音");
//		suggestionBuilder.size(20);
//		SuggestResponse suggestResponse = elasticsearchTemplate.suggest(suggestionBuilder, Book.class);
//		Suggest suggest = suggestResponse.getSuggest();
//		Suggestion<? extends Entry<? extends Option>> suggestion = suggest.getSuggestion("mySuggestion");
//		List<? extends Entry<? extends Option>> entries = suggestion.getEntries();
//		for (Entry<? extends Option> entry : entries) {
//			System.out.println(entry.getText().string());
//		}
	}
	
	private void print(Book book) {
		System.out.println("----------------------------");
		System.out.println(book);
	}
	
	
}
